//+------------------------------------------------------------------+
//| CopilotEA.mq5                                                    |
//| MT5 <-> Telegram command relay EA — DEMO ONLY trial              |
//|                                                                  |
//| **********************  DEMO LOCK  ****************************  |
//| * This EA REFUSES TO RUN on anything except a DEMO account.    * |
//| * OnInit checks the account trade mode; if it is NOT demo,     * |
//| * the EA alerts, prints a refusal and FAILS TO START.          * |
//| * It also re-checks before EVERY command it executes.          * |
//| * NEVER attach it to a live account "just to test".            * |
//| ****************************************************************  |
//|                                                                  |
//| WHAT THIS EA DOES                                                |
//|   * Reports position opened / SL or TP changed / position       |
//|     closed / every deal / heartbeat to a watcher endpoint, and  |
//|     streams the last 3 closed candles on every new bar.         |
//|   * PLUS, only on a DEMO account and only if you deliberately   |
//|     set InpAllowExecution = true when attaching it:             |
//|     it polls the watcher for commands from your bot/operator    |
//|     and executes them — ENTER (market order with SL/TP),        |
//|     SET_SLTp (move a position's stop/target), CLOSE (close one  |
//|     position), CLOSE_ALL (close everything). Each command is    |
//|     printed to the Experts log BEFORE it runs, and the outcome  |
//|     is reported back to the watcher (ack) after it runs.        |
//|                                                                  |
//| WHAT THIS EA NEVER DOES                                          |
//|   * Never runs on a live or contest account (demo lock above).  |
//|   * Never trades on its own: no strategy, no signals, no        |
//|     automatic trade management of any kind — it only executes   |
//|     explicit commands that arrive from the watcher, and every   |
//|     command expires at the watcher after ~20 seconds.           |
//|   * Never exceeds InpMaxVolume on an ENTER command.             |
//|   * Never sees or sends the account password.                   |
//|                                                                  |
//| SETUP (review this source before running it)                     |
//|   1. DEMO account only. Compile in MetaEditor (F7).             |
//|   2. Tools > Options > Expert Advisors: allow WebRequest for    |
//|      your watcher base URL.                                     |
//|   3. Attach to a chart; set InpWatcherURL + InpSharedSecret;    |
//|      set InpAllowExecution = true ONLY if you want the trial    |
//|      to execute commands.                                       |
//+------------------------------------------------------------------+
#property copyright   "MT5 <-> Telegram Command Relay EA — DEMO ONLY trial"
#property version     "1.00"
#property strict

#include <Trade/Trade.mqh>

//------------------------- USER SETTINGS ---------------------------
input string InpWatcherURL       = "";     // Watcher REPORT URL (same as Reporter; empty = LOCAL MODE, log only)
input string InpCommandsURL      = "";     // Commands URL (…/commands/pending); empty = derive from InpWatcherURL
input string InpSharedSecret     = "";     // Shared secret token (set by whoever runs the watcher; keep it out of chat/screenshots)
input bool   InpAllowExecution   = false;  // MASTER SWITCH: execute commands? (deliberately default OFF)
input double InpMaxVolume        = 0.10;   // Hard cap on any ENTER volume
input ulong  InpMagicNumber      = 991016; // Magic number stamped on co-pilot orders
input int    InpPollSeconds      = 3;      // Position poll interval (seconds)
input int    InpHeartbeatSecs    = 60;     // Heartbeat interval (seconds)
input bool   InpLocalAlerts      = true;   // Popup alert on position open/close + commands
input string InpAccountTag       = "";     // Optional label, e.g. "demo"
//-------------------------------------------------------------------

CTrade   g_trade;
string   g_fileName = "copilot-ea-log.txt";
long     g_lastHeartbeat = 0;
long     g_timerTicks = 0;
datetime g_lastBarTime = 0;
long     g_lastHttpErrPrint = 0;
bool     g_startEventPending = true;   // COPILOT_START is sent on the first
                                       // timer tick — WebRequest is not
                                       // reliable inside OnInit

// Snapshot of one open position, for change detection (same as Reporter)
struct PosSnap
{
   ulong  ticket;
   long   posId;
   string symbol;
   long   type;        // POSITION_TYPE_BUY / SELL
   double volume;
   double priceOpen;
   double sl;
   double tp;
   double profit;
};

PosSnap g_prev[];   // positions seen on the previous poll

//------------------------- SMALL HELPERS ---------------------------
string JsonEsc(string s)
{
   StringReplace(s, "\\", "\\\\");
   StringReplace(s, "\"", "\\\"");
   return s;
}

string NowIso()
{
   return TimeToString(TimeCurrent(), TIME_DATE | TIME_SECONDS);
}

bool IsDemoAccount()
{
   ENUM_ACCOUNT_TRADE_MODE mode =
      (ENUM_ACCOUNT_TRADE_MODE)AccountInfoInteger(ACCOUNT_TRADE_MODE);
   return(mode == ACCOUNT_TRADE_MODE_DEMO);
}

string BaseJson(string eventName)
{
   string j = "{";
   j += "\"event\":\"" + eventName + "\",";
   j += "\"account_login\":" + IntegerToString(AccountInfoInteger(ACCOUNT_LOGIN)) + ",";
   j += "\"account_server\":\"" + JsonEsc(AccountInfoString(ACCOUNT_SERVER)) + "\",";
   j += "\"account_tag\":\"" + JsonEsc(InpAccountTag) + "\",";
   j += "\"time_server\":\"" + NowIso() + "\",";
   j += "\"balance\":" + DoubleToString(AccountInfoDouble(ACCOUNT_BALANCE), 2) + ",";
   j += "\"equity\":" + DoubleToString(AccountInfoDouble(ACCOUNT_EQUITY), 2);
   return j;
}

//------------------------- HTTP PLUMBING ---------------------------
string AuthHeaders()
{
   string h = "Content-Type: application/json\r\n";
   if(InpSharedSecret != "")
      h += "X-Reporter-Token: " + InpSharedSecret + "\r\n";
   return h;
}

// POST a JSON body; returns the HTTP status code (or -1 on transport error).
int HttpPostJson(string url, string json, string &responseBody)
{
   responseBody = "";
   if(url == "") return -1;
   char postData[];
   char result[];
   string resultHeaders;
   StringToCharArray(json, postData, 0, StringLen(json), CP_UTF8);
   ResetLastError();
   int res = WebRequest("POST", url, AuthHeaders(), 5000, postData, result, resultHeaders);
   if(res == -1)
   {
      long now = (long)TimeCurrent();
      if(now - g_lastHttpErrPrint >= 30)
      {
         g_lastHttpErrPrint = now;
         Print("[CopilotEA] WebRequest POST FAILED, error ", GetLastError(),
               " — is the watcher URL whitelisted in Tools > Options > Expert Advisors?");
      }
   }
   else
      responseBody = CharArrayToString(result, 0, WHOLE_ARRAY, CP_UTF8);
   return res;
}

// GET a URL; returns the HTTP status code (or -1 on transport error).
int HttpGet(string url, string &responseBody)
{
   responseBody = "";
   if(url == "") return -1;
   char emptyData[];
   char result[];
   string resultHeaders;
   ResetLastError();
   int res = WebRequest("GET", url, AuthHeaders(), 5000, emptyData, result, resultHeaders);
   if(res == -1)
   {
      long now = (long)TimeCurrent();
      if(now - g_lastHttpErrPrint >= 30)
      {
         g_lastHttpErrPrint = now;
         Print("[CopilotEA] WebRequest GET FAILED, error ", GetLastError(),
               " — is the watcher URL whitelisted in Tools > Options > Expert Advisors?");
      }
   }
   else
      responseBody = CharArrayToString(result, 0, WHOLE_ARRAY, CP_UTF8);
   return res;
}

// Watcher base URL (…/report and …/commands/… live under the same base).
string WatcherBase()
{
   string suffix = "/commands/pending";
   if(StringLen(InpCommandsURL) > StringLen(suffix) &&
      StringSubstr(InpCommandsURL, StringLen(InpCommandsURL) - StringLen(suffix)) == suffix)
      return StringSubstr(InpCommandsURL, 0, StringLen(InpCommandsURL) - StringLen(suffix));
   suffix = "/report";
   if(StringLen(InpWatcherURL) > StringLen(suffix) &&
      StringSubstr(InpWatcherURL, StringLen(InpWatcherURL) - StringLen(suffix)) == suffix)
      return StringSubstr(InpWatcherURL, 0, StringLen(InpWatcherURL) - StringLen(suffix));
   return "";
}

string CommandsPendingURL()
{
   if(InpCommandsURL != "") return InpCommandsURL;
   string base = WatcherBase();
   return (base == "") ? "" : base + "/commands/pending";
}

string CommandsAckURL()
{
   string base = WatcherBase();
   return (base == "") ? "" : base + "/commands/ack";
}

// Send one event: to the watcher URL if set, otherwise to the log file.
void Emit(string json)
{
   Print("[CopilotEA] ", json);

   if(InpWatcherURL == "")
   {
      int h = FileOpen(g_fileName, FILE_READ | FILE_WRITE | FILE_TXT | FILE_ANSI);
      if(h != INVALID_HANDLE)
      {
         FileSeek(h, 0, SEEK_END);
         FileWriteString(h, json + "\r\n");
         FileClose(h);
      }
      return;
   }

   string resp;
   HttpPostJson(InpWatcherURL, json, resp);
}

//------------------------- JSON READING ----------------------------
// Minimal extractors for the flat command objects the watcher sends.
// Find "key" then ':'; read a quoted string ("" if null/missing).
string GetJsonString(string obj, string key)
{
   string pat = "\"" + key + "\"";
   int p = StringFind(obj, pat);
   if(p < 0) return "";
   p = StringFind(obj, ":", p + StringLen(pat));
   if(p < 0) return "";
   p++;
   while(p < StringLen(obj) && StringGetCharacter(obj, p) == ' ') p++;
   if(p >= StringLen(obj) || StringGetCharacter(obj, p) != '"') return "";
   p++;
   string out = "";
   while(p < StringLen(obj))
   {
      ushort c = StringGetCharacter(obj, p);
      if(c == '\\' && p + 1 < StringLen(obj))
      {
         ushort n = StringGetCharacter(obj, p + 1);
         if(n == '"' || n == '\\' || n == '/') { out += CharToString((uchar)n); p += 2; continue; }
         if(n == 'n') { out += "\n"; p += 2; continue; }
         if(n == 't') { out += "\t"; p += 2; continue; }
      }
      if(c == '"') break;
      out += CharToString((uchar)c);
      p++;
   }
   return out;
}

// Find "key" then ':'; read the raw number token ("" if null/missing).
string GetJsonNumberStr(string obj, string key)
{
   string pat = "\"" + key + "\"";
   int p = StringFind(obj, pat);
   if(p < 0) return "";
   p = StringFind(obj, ":", p + StringLen(pat));
   if(p < 0) return "";
   p++;
   while(p < StringLen(obj) && StringGetCharacter(obj, p) == ' ') p++;
   string out = "";
   while(p < StringLen(obj))
   {
      ushort c = StringGetCharacter(obj, p);
      if((c >= '0' && c <= '9') || c == '-' || c == '+' || c == '.' || c == 'e' || c == 'E')
      {
         out += CharToString((uchar)c);
         p++;
      }
      else break;
   }
   return out;
}

double GetJsonDouble(string obj, string key, double def)
{
   string s = GetJsonNumberStr(obj, key);
   if(s == "") return def;
   return StringToDouble(s);
}

long GetJsonLong(string obj, string key, long def)
{
   string s = GetJsonNumberStr(obj, key);
   if(s == "") return def;
   return StringToInteger(s);
}

// Split the "commands":[ {...}, {...} ] array into its flat objects.
int SplitCommandObjects(string body, string &objects[])
{
   ArrayResize(objects, 0);
   int arrStart = StringFind(body, "\"commands\"");
   if(arrStart < 0) return 0;
   arrStart = StringFind(body, "[", arrStart);
   if(arrStart < 0) return 0;

   int depth = 0;
   int objStart = -1;
   bool inStr = false;
   bool esc = false;
   for(int i = arrStart + 1; i < StringLen(body); i++)
   {
      ushort c = StringGetCharacter(body, i);
      if(inStr)
      {
         if(esc) esc = false;
         else if(c == '\\') esc = true;
         else if(c == '"') inStr = false;
         continue;
      }
      if(c == '"') { inStr = true; continue; }
      if(c == '{')
      {
         if(depth == 0) objStart = i;
         depth++;
      }
      else if(c == '}')
      {
         depth--;
         if(depth == 0 && objStart >= 0)
         {
            int n = ArraySize(objects);
            ArrayResize(objects, n + 1);
            objects[n] = StringSubstr(body, objStart, i - objStart + 1);
            objStart = -1;
         }
      }
      else if(c == ']' && depth == 0)
         break;
   }
   return ArraySize(objects);
}

//------------------------- POSITION HELPERS ------------------------
string PositionJson(string eventName, PosSnap &p)
{
   string j = BaseJson(eventName);
   j += ",\"position_ticket\":" + IntegerToString((long)p.ticket);
   j += ",\"position_id\":" + IntegerToString(p.posId);
   j += ",\"symbol\":\"" + JsonEsc(p.symbol) + "\"";
   j += ",\"side\":\"" + (p.type == POSITION_TYPE_BUY ? "BUY" : "SELL") + "\"";
   j += ",\"volume\":" + DoubleToString(p.volume, 2);
   j += ",\"price_open\":" + DoubleToString(p.priceOpen, _Digits);
   j += ",\"sl\":" + DoubleToString(p.sl, _Digits);
   j += ",\"tp\":" + DoubleToString(p.tp, _Digits);
   j += ",\"profit\":" + DoubleToString(p.profit, 2);
   j += "}";
   return j;
}

int SnapshotPositions(PosSnap &out[])
{
   int total = PositionsTotal();
   ArrayResize(out, total);
   for(int i = 0; i < total; i++)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0) continue;
      out[i].ticket    = ticket;
      out[i].posId     = PositionGetInteger(POSITION_IDENTIFIER);
      out[i].symbol    = PositionGetString(POSITION_SYMBOL);
      out[i].type      = PositionGetInteger(POSITION_TYPE);
      out[i].volume    = PositionGetDouble(POSITION_VOLUME);
      out[i].priceOpen = PositionGetDouble(POSITION_PRICE_OPEN);
      out[i].sl        = PositionGetDouble(POSITION_SL);
      out[i].tp        = PositionGetDouble(POSITION_TP);
      out[i].profit    = PositionGetDouble(POSITION_PROFIT);
   }
   return total;
}

int FindPrev(ulong ticket)
{
   for(int i = 0; i < ArraySize(g_prev); i++)
      if(g_prev[i].ticket == ticket) return i;
   return -1;
}

void PollPositions()
{
   PosSnap now[];
   int n = SnapshotPositions(now);

   // New positions + SL/TP changes
   for(int i = 0; i < n; i++)
   {
      int k = FindPrev(now[i].ticket);
      if(k < 0)
      {
         Emit(PositionJson("POSITION_OPEN", now[i]));
         if(InpLocalAlerts)
            Alert("Copilot EA: NEW POSITION ", now[i].symbol, " ",
                  (now[i].type == POSITION_TYPE_BUY ? "BUY" : "SELL"), " ",
                  DoubleToString(now[i].volume, 2), " @ ",
                  DoubleToString(now[i].priceOpen, _Digits));
      }
      else if(MathAbs(g_prev[k].sl - now[i].sl) > 0.0000001 ||
              MathAbs(g_prev[k].tp - now[i].tp) > 0.0000001)
      {
         Emit(PositionJson("POSITION_SLTP_CHANGE", now[i]));
      }
   }

   // Closed positions (were in prev, gone now)
   for(int i = 0; i < ArraySize(g_prev); i++)
   {
      bool stillThere = false;
      for(int j = 0; j < n; j++)
         if(now[j].ticket == g_prev[i].ticket) { stillThere = true; break; }
      if(!stillThere)
      {
         Emit(PositionJson("POSITION_CLOSE", g_prev[i]));
         if(InpLocalAlerts)
            Alert("Copilot EA: POSITION CLOSED ", g_prev[i].symbol);
      }
   }

   ArrayResize(g_prev, n);
   for(int i = 0; i < n; i++) g_prev[i] = now[i];
}

// Find a position ticket from the id the operator sends (position_id from the
// reporter events = POSITION_IDENTIFIER; a raw ticket also matches).
ulong FindPositionTicket(long wantId)
{
   if(wantId <= 0) return 0;
   for(int i = 0; i < PositionsTotal(); i++)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0) continue;
      long ident = PositionGetInteger(POSITION_IDENTIFIER);
      if((long)ticket == wantId || ident == wantId)
         return ticket;
   }
   return 0;
}

// Identifier of the most recently opened position on a symbol (for acks).
long LatestPositionId(string symbol)
{
   long bestId = 0;
   long bestTime = -1;
   for(int i = 0; i < PositionsTotal(); i++)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0) continue;
      if(PositionGetString(POSITION_SYMBOL) != symbol) continue;
      long t = PositionGetInteger(POSITION_TIME_MSC);
      if(t > bestTime)
      {
         bestTime = t;
         bestId = PositionGetInteger(POSITION_IDENTIFIER);
      }
   }
   return bestId;
}

//------------------------- TRADE EVENTS ----------------------------
void OnTradeTransaction(const MqlTradeTransaction &trans,
                        const MqlTradeRequest &request,
                        const MqlTradeResult &result)
{
   if(trans.type != TRADE_TRANSACTION_DEAL_ADD) return;
   if(trans.deal == 0) return;
   if(!HistoryDealSelect(trans.deal)) return;

   long entry = HistoryDealGetInteger(trans.deal, DEAL_ENTRY);
   string entryName = (entry == DEAL_ENTRY_IN)  ? "IN"  :
                      (entry == DEAL_ENTRY_OUT) ? "OUT" : "INOUT";

   string j = BaseJson("DEAL");
   j += ",\"deal_ticket\":" + IntegerToString((long)trans.deal);
   j += ",\"position_id\":" + IntegerToString(HistoryDealGetInteger(trans.deal, DEAL_POSITION_ID));
   j += ",\"symbol\":\"" + JsonEsc(HistoryDealGetString(trans.deal, DEAL_SYMBOL)) + "\"";
   j += ",\"deal_type\":" + IntegerToString(HistoryDealGetInteger(trans.deal, DEAL_TYPE));
   j += ",\"entry\":\"" + entryName + "\"";
   j += ",\"price\":" + DoubleToString(HistoryDealGetDouble(trans.deal, DEAL_PRICE), _Digits);
   j += ",\"volume\":" + DoubleToString(HistoryDealGetDouble(trans.deal, DEAL_VOLUME), 2);
   j += ",\"profit\":" + DoubleToString(HistoryDealGetDouble(trans.deal, DEAL_PROFIT), 2);
   j += "}";
   Emit(j);
}

//------------------------- CANDLE STREAM ---------------------------
string TimeframeName()
{
   string s = EnumToString(PERIOD_CURRENT);   // e.g. "PERIOD_M15"
   if(StringSubstr(s, 0, 7) == "PERIOD_")
      s = StringSubstr(s, 7);
   return s;
}

// On every new bar of the chart: send the last 3 CLOSED candles.
void CheckNewBar()
{
   datetime t = iTime(_Symbol, PERIOD_CURRENT, 0);
   if(t == 0) return;
   if(t == g_lastBarTime) return;
   bool first = (g_lastBarTime == 0);
   g_lastBarTime = t;

   if(Bars(_Symbol, PERIOD_CURRENT) < 4) return;

   string j = BaseJson("CANDLE");
   j += ",\"symbol\":\"" + JsonEsc(_Symbol) + "\"";
   j += ",\"timeframe\":\"" + TimeframeName() + "\"";
   j += ",\"candles\":[";
   bool wroteAny = false;
   for(int shift = 3; shift >= 1; shift--)   // oldest first
   {
      datetime ct = iTime(_Symbol, PERIOD_CURRENT, shift);
      double o = iOpen(_Symbol, PERIOD_CURRENT, shift);
      double h = iHigh(_Symbol, PERIOD_CURRENT, shift);
      double l = iLow(_Symbol, PERIOD_CURRENT, shift);
      double c = iClose(_Symbol, PERIOD_CURRENT, shift);
      if(ct == 0) continue;
      if(wroteAny) j += ",";
      j += "{\"time\":\"" + TimeToString(ct, TIME_DATE | TIME_SECONDS) + "\"";
      j += ",\"open\":" + DoubleToString(o, _Digits);
      j += ",\"high\":" + DoubleToString(h, _Digits);
      j += ",\"low\":" + DoubleToString(l, _Digits);
      j += ",\"close\":" + DoubleToString(c, _Digits) + "}";
      wroteAny = true;
   }
   j += "]}";
   if(wroteAny) Emit(j);
   if(first) Print("[CopilotEA] Candle streaming started on ", _Symbol, " ", TimeframeName());
}

//------------------------- COMMAND EXECUTION -----------------------
void AckCommand(string id, string status, string detail)
{
   Print("[CopilotEA] COMMAND RESULT id=", id, " status=", status, " — ", detail);
   string url = CommandsAckURL();
   if(url == "") return;
   string j = "{\"id\":\"" + JsonEsc(id) + "\",\"status\":\"" + status +
              "\",\"detail\":\"" + JsonEsc(detail) + "\"}";
   string resp;
   int code = HttpPostJson(url, j, resp);
   if(code != 200)
      Print("[CopilotEA] Ack POST returned HTTP ", code, " for command ", id);
}

bool RetcodeOk(uint rc)
{
   return(rc == TRADE_RETCODE_DONE || rc == TRADE_RETCODE_DONE_PARTIAL ||
          rc == TRADE_RETCODE_PLACED);
}

void ExecuteEnter(string id, string symbol, string sideRaw, double volume,
                  double sl, double tp)
{
   string side = sideRaw;
   StringToUpper(side);
   if(symbol == "") { AckCommand(id, "rejected", "ENTER with no symbol"); return; }
   if(side != "BUY" && side != "SELL") { AckCommand(id, "rejected", "ENTER side must be BUY or SELL, got '" + sideRaw + "'"); return; }
   if(volume <= 0) { AckCommand(id, "rejected", "ENTER with no/zero volume"); return; }
   if(volume > InpMaxVolume)
   {
      AckCommand(id, "rejected", "volume " + DoubleToString(volume, 2) +
                 " exceeds InpMaxVolume cap " + DoubleToString(InpMaxVolume, 2));
      return;
   }
   if(!SymbolSelect(symbol, true))
   {
      AckCommand(id, "failed", "SymbolSelect failed for " + symbol);
      return;
   }

   int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);
   double minVol = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MIN);
   double maxVol = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MAX);
   double stepVol = SymbolInfoDouble(symbol, SYMBOL_VOLUME_STEP);
   if(volume < minVol)
   {
      AckCommand(id, "rejected", "volume " + DoubleToString(volume, 2) +
                 " below symbol minimum " + DoubleToString(minVol, 2));
      return;
   }
   if(volume > maxVol) volume = maxVol;
   if(stepVol > 0)
   {
      volume = MathFloor(volume / stepVol) * stepVol;
      int volDigits = (stepVol >= 1.0) ? 0 : (stepVol >= 0.1) ? 1 : (stepVol >= 0.01) ? 2 : 3;
      volume = NormalizeDouble(volume, volDigits);
   }
   double slN = (sl > 0) ? NormalizeDouble(sl, digits) : 0.0;
   double tpN = (tp > 0) ? NormalizeDouble(tp, digits) : 0.0;

   bool ok = (side == "BUY")
      ? g_trade.Buy(volume, symbol, 0.0, slN, tpN, "Copilot ENTER")
      : g_trade.Sell(volume, symbol, 0.0, slN, tpN, "Copilot ENTER");
   uint rc = g_trade.ResultRetcode();
   if(ok && RetcodeOk(rc))
   {
      long posId = LatestPositionId(symbol);
      AckCommand(id, "executed",
                 "filled price=" + DoubleToString(g_trade.ResultPrice(), digits) +
                 " order=" + IntegerToString((long)g_trade.ResultOrder()) +
                 " deal=" + IntegerToString((long)g_trade.ResultDeal()) +
                 " position_id=" + IntegerToString(posId) +
                 " volume=" + DoubleToString(volume, 2));
   }
   else
   {
      AckCommand(id, "failed",
                 "retcode=" + IntegerToString((long)rc) + " " +
                 g_trade.ResultRetcodeDescription());
   }
}

void ExecuteSetSlTp(string id, string obj)
{
   long positionId = GetJsonLong(obj, "position_id", 0);
   if(positionId <= 0) { AckCommand(id, "rejected", "SET_SLTp with no position_id"); return; }
   ulong ticket = FindPositionTicket(positionId);
   if(ticket == 0)
   {
      AckCommand(id, "failed", "position " + IntegerToString(positionId) + " not found (already closed?)");
      return;
   }
   if(!PositionSelectByTicket(ticket))
   {
      AckCommand(id, "failed", "PositionSelectByTicket failed for ticket " + IntegerToString((long)ticket));
      return;
   }
   string symbol = PositionGetString(POSITION_SYMBOL);
   int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);
   // A level ABSENT from the command keeps the position's current level —
   // "move my stop" must never silently erase the target. A level sent
   // explicitly as 0 removes it.
   string slRaw = GetJsonNumberStr(obj, "sl");
   string tpRaw = GetJsonNumberStr(obj, "tp");
   double slN = (slRaw == "") ? PositionGetDouble(POSITION_SL)
                               : NormalizeDouble(StringToDouble(slRaw), digits);
   double tpN = (tpRaw == "") ? PositionGetDouble(POSITION_TP)
                               : NormalizeDouble(StringToDouble(tpRaw), digits);

   bool ok = g_trade.PositionModify(ticket, slN, tpN);
   uint rc = g_trade.ResultRetcode();
   if(ok && RetcodeOk(rc))
      AckCommand(id, "executed", "position_id=" + IntegerToString(positionId) +
                 " sl=" + DoubleToString(slN, digits) +
                 " tp=" + DoubleToString(tpN, digits));
   else
      AckCommand(id, "failed", "retcode=" + IntegerToString((long)rc) + " " +
                 g_trade.ResultRetcodeDescription());
}

void ExecuteClose(string id, long positionId)
{
   if(positionId <= 0) { AckCommand(id, "rejected", "CLOSE with no position_id"); return; }
   ulong ticket = FindPositionTicket(positionId);
   if(ticket == 0)
   {
      AckCommand(id, "failed", "position " + IntegerToString(positionId) + " not found (already closed?)");
      return;
   }
   bool ok = g_trade.PositionClose(ticket);
   uint rc = g_trade.ResultRetcode();
   if(ok && RetcodeOk(rc))
      AckCommand(id, "executed", "closed position_id=" + IntegerToString(positionId) +
                 " deal=" + IntegerToString((long)g_trade.ResultDeal()));
   else
      AckCommand(id, "failed", "retcode=" + IntegerToString((long)rc) + " " +
                 g_trade.ResultRetcodeDescription());
}

void ExecuteCloseAll(string id)
{
   ulong tickets[];
   int total = PositionsTotal();
   ArrayResize(tickets, total);
   for(int i = 0; i < total; i++)
      tickets[i] = PositionGetTicket(i);

   int closed = 0;
   int failed = 0;
   for(int i = 0; i < total; i++)
   {
      if(tickets[i] == 0) continue;
      if(g_trade.PositionClose(tickets[i]) && RetcodeOk(g_trade.ResultRetcode()))
         closed++;
      else
         failed++;
   }
   if(failed == 0)
      AckCommand(id, "executed", "closed " + IntegerToString(closed) + " position(s)");
   else
      AckCommand(id, "failed", "closed " + IntegerToString(closed) +
                 ", failed " + IntegerToString(failed) + " — check Experts log");
}

void HandleCommand(string obj)
{
   string id = GetJsonString(obj, "id");
   string type = GetJsonString(obj, "type");
   if(id == "") { Print("[CopilotEA] Command object without id — ignored: ", obj); return; }

   // AUDIT: the full command is printed BEFORE anything is executed.
   Print("[CopilotEA] COMMAND RECEIVED: ", obj);
   if(InpLocalAlerts)
      Alert("Copilot EA: command received — ", type, " (see Experts log)");

   if(!InpAllowExecution)
   {
      AckCommand(id, "rejected", "execution disabled: InpAllowExecution=false at attach time");
      return;
   }
   if(!IsDemoAccount())
   {
      AckCommand(id, "rejected", "account is NOT demo — co-pilot execution is locked to demo");
      return;
   }

   string t = type;
   StringToUpper(t);
   if(t == "ENTER")
   {
      ExecuteEnter(id,
                   GetJsonString(obj, "symbol"),
                   GetJsonString(obj, "side"),
                   GetJsonDouble(obj, "volume", 0.0),
                   GetJsonDouble(obj, "sl", 0.0),
                   GetJsonDouble(obj, "tp", 0.0));
   }
   else if(t == "SET_SLTP")
   {
      ExecuteSetSlTp(id, obj);
   }
   else if(t == "CLOSE")
   {
      ExecuteClose(id, GetJsonLong(obj, "position_id", 0));
   }
   else if(t == "CLOSE_ALL")
   {
      ExecuteCloseAll(id);
   }
   else
   {
      AckCommand(id, "rejected", "unknown command type '" + type + "'");
   }
}

void PollCommands()
{
   string url = CommandsPendingURL();
   if(url == "") return;
   string body;
   int code = HttpGet(url, body);
   if(code != 200 || body == "") return;

   string objects[];
   int n = SplitCommandObjects(body, objects);
   for(int i = 0; i < n; i++)
      HandleCommand(objects[i]);
}

//------------------------- LIFECYCLE -------------------------------
int OnInit()
{
   // ============ DEMO LOCK — the EA refuses to start otherwise ====
   if(!IsDemoAccount())
   {
      string msg = "CopilotEA REFUSES TO RUN: this account is NOT a demo account " +
                   "(login " + IntegerToString(AccountInfoInteger(ACCOUNT_LOGIN)) +
                   " on " + AccountInfoString(ACCOUNT_SERVER) + "). " +
                   "The co-pilot trial is DEMO ONLY. EA will not start.";
      Print("[CopilotEA] ", msg);
      Alert(msg);
      return(INIT_FAILED);
   }
   // ================================================================

   EventSetTimer(1);
   g_trade.SetExpertMagicNumber(InpMagicNumber);
   g_trade.SetDeviationInPoints(100);
   SnapshotPositions(g_prev);

   string mode = (InpWatcherURL == "") ? "LOCAL MODE (log file only — commands unavailable)"
                                        : "POSTING to watcher";
   Print("[CopilotEA] STARTED on DEMO account ", AccountInfoInteger(ACCOUNT_LOGIN),
         " on ", AccountInfoString(ACCOUNT_SERVER),
         " | ", mode,
         " | execution ", (InpAllowExecution ? "ENABLED" : "DISABLED (InpAllowExecution=false)"),
         " | max volume ", DoubleToString(InpMaxVolume, 2),
         " | commands URL: ", (CommandsPendingURL() == "" ? "(none)" : CommandsPendingURL()),
         " | open positions at start: ", ArraySize(g_prev));
   if(!InpAllowExecution)
      Print("[CopilotEA] NOTE: commands will be REJECTED until InpAllowExecution=true.");
   g_startEventPending = true;   // COPILOT_START goes out on the first OnTimer
   return(INIT_SUCCEEDED);
}

void OnDeinit(const int reason)
{
   EventKillTimer();
   Emit(BaseJson("COPILOT_STOP") + "}");
   Print("[CopilotEA] STOPPED, reason ", reason);
}

void OnTick()
{
   CheckNewBar();
}

void OnTimer()
{
   g_timerTicks++;
   if(g_startEventPending)
   {
      g_startEventPending = false;
      Emit(BaseJson("COPILOT_START") + ",\"mode\":\"" +
           (InpWatcherURL == "" ? "local" : "watcher") +
           "\",\"execution_enabled\":" + (InpAllowExecution ? "true" : "false") + "}");
   }
   PollCommands();
   int pollEvery = MathMax(1, InpPollSeconds);
   if(g_timerTicks % pollEvery == 0)
      PollPositions();
   long now = (long)TimeCurrent();
   if(now - g_lastHeartbeat >= InpHeartbeatSecs)
   {
      g_lastHeartbeat = now;
      Emit(BaseJson("HEARTBEAT") + ",\"open_positions\":" +
           IntegerToString(PositionsTotal()) + "}");
   }
}
//+------------------------------------------------------------------+
