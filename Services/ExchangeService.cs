using ArbitrageScanner.Interfaces;
using ArbitrageScanner.Models;
using ccxt;
using NLog;
using System.Data;

namespace ArbitrageScanner.Services
{
    public class ExchangeService : IExchangeService, IDisposable
    {
        private readonly Logger logger;
        private readonly Random random = new();
        private readonly CancellationTokenSource cts = new();
        public delegate void putOrderBookHandler(string exchangeId, VerifyOrderBook orderBook);
        public event Interfaces.putOrderBookHandler? EventOrderBook;

        // List of exchanges with fees (taker fee), exchanges, and symbol queues
        // The value of 'taker fee' depends on your data plan on the exchange
        // Attention! Not all exchanges provide data without registering an account and obtaining API keys.
        public Dictionary<string, (decimal fee, Exchange exchange, Queue<string> symbolQueue)> Exchanges { get; } = new()
        {
            { "kraken", (0.0026m, new Kraken(), new()) },
            { "gate", (0.002m, new gate(), new()) },
            { "bitmex", (0.001m, new bitmex(), new()) },
            { "bitget", (0.001m, new bitget(), new()) },
            { "xt", (0.002m, new xt(), new()) },
            { "bitfinex", (0.001m, new bitfinex(), new()) },
            { "bitmart", (0.001m, new bitmart(), new()) },
            { "coinbase", (0.001m, new coinbase(), new()) },
            { "coinex", (0.001m, new coinex(), new()) },
            { "hashkey", (0.001m, new hashkey(), new()) },
            { "hyperliquid", (0.001m, new hyperliquid(), new()) },
            { "htx", (0.001m, new htx(), new()) },
            { "kucoin", (0.001m, new kucoin(), new()) },
            { "whitebit", (0.001m, new whitebit(), new()) },
            { "exmo", (0.001m, new exmo(), new()) },
            { "poloniex",(0.002m, new poloniex(), new()) }
        };
        public ExchangeService(Logger _logger)
        {
            logger = _logger;
            Init();
        }
        private void Init()
        {
            foreach (var exchangeId in Exchanges.Keys)
            {
                StartExchangeTask(exchangeId, cts.Token);
            }
        }
        private void StartExchangeTask(string exchangeId, CancellationToken token)
        {
            // Start a task to poll the queue
            Task.Run(async () =>
            {
                if (Exchanges.TryGetValue(exchangeId, out var exchangeData))
                {
                    var (fee, exchange, queue) = exchangeData;
                    if (exchange != null && queue != null)
                        while (!token.IsCancellationRequested)
                        {
                            try
                            {
                                if (queue.TryDequeue(out string? symbol))
                                {
                                    var orderBook = await GetOrderBook(exchangeId, symbol);
                                    if (orderBook != null)
                                        EventOrderBook?.Invoke(exchangeId, orderBook, token);
                                }
                                else
                                    // Add a delay before the next queue polling
                                    await Task.Delay(random.Next(200, 501));
                            }
                            catch (Exception ex)
                            {
                                logger.Error(ex, $"Error in thread for exchange {exchangeId}");
                            }
                        }
                }
            }, token);
        }
        public async Task<DataTable> GetMarketData(string exchangeId)
        {
            var data = new DataTable();
            data.Columns.AddRange(
            [
                new DataColumn("exchange_name", typeof(string)),
                new DataColumn("symbol", typeof(string)),
                new DataColumn("base_currency", typeof(string)),
                new DataColumn("quote_currency", typeof(string)),
                new DataColumn("type", typeof(string)),
                new DataColumn("expiry", typeof(string)),
                new DataColumn("expiry_datetime", typeof(DateTime)),
                new DataColumn("is_active", typeof(byte)), // Use byte for Nullable(UInt8)
                new DataColumn("updated_time", typeof(DateTime))
            ]);

            try
            {
                // Get data from the exchange
                DateTime dateTime = DateTime.Now;
                var markets = await Exchanges[exchangeId].exchange.loadMarkets();

                if (markets is Dictionary<string, object> dict)
                {
                    foreach (var item in dict)
                    {
                        if (item.Value is Dictionary<string, object> itemDict)
                        {
                            // Map is_active to byte (0/1) or DBNull.Value
                            object isActive;
                            if (!itemDict.ContainsKey("active") || itemDict["active"] == null)
                            {
                                isActive = DBNull.Value;
                            }
                            else
                            {
                                var activeStr = itemDict["active"].ToString()?.ToLower();
                                if (activeStr == "true")
                                    isActive = (byte)1;
                                else if (activeStr == "false")
                                    isActive = (byte)0;
                                else
                                    isActive = DBNull.Value;
                            }

                            // Helper to get value or DBNull.Value
                            object GetOrDbNull(string key)
                            {
                                return itemDict.ContainsKey(key) && itemDict[key] != null ? itemDict[key] : DBNull.Value;
                            }

                            data.Rows.Add(
                            [
                                exchangeId,
                                GetOrDbNull("symbol"),
                                GetOrDbNull("base"),
                                GetOrDbNull("quote"),
                                GetOrDbNull("type"),
                                GetOrDbNull("expiry"),
                                GetOrDbNull("expiryDatetime"),
                                isActive,
                                dateTime
                            ]);
                        }
                    }
                }

                logger.Info($"{data.Rows.Count} pairs loaded for exchange {Exchanges[exchangeId].exchange.name}.");
            }
            catch (Exception ex)
            {
                logger.Error(ex, $"Error in method 'GetMarketsData', exchange {exchangeId}: {ex}");
            }
            return data;
        }
        public async Task<DataTable> GetTickers(string exchangeId)
        {
            var data = new DataTable();
            data.Columns.AddRange(
            [
                new DataColumn("exchange_name", typeof(string)),
                new DataColumn("symbol", typeof(string)),
                new DataColumn("base_currency", typeof(string)),
                new DataColumn("quote_currency", typeof(string)),
                new DataColumn("event_time", typeof(DateTime)),
                new DataColumn("bid_price", typeof(double)),
                new DataColumn("ask_price", typeof(double)),
                new DataColumn("quote_volume", typeof(double)),
                new DataColumn("updated_time", typeof(DateTime))
            ]);

            try
            {
                int emptyData = 0;
                DateTime eventTime = DateTime.Now;
                DateTime updatedTime = eventTime;
                var tickers = await Exchanges[exchangeId].exchange.FetchTickers();

                if (tickers.tickers is Dictionary<string, Ticker> _tickers)
                {
                    foreach (var _ticker in _tickers)
                    {
                        // Prepare data for insertion
                        var currencies = _ticker.Key.Split("/");
                        if (currencies.Length < 2)
                        {
                            emptyData++;
                            continue;
                        }

                        // If the date is not filled, use the current date
                        if (!string.IsNullOrEmpty(_ticker.Value.datetime) && DateTime.TryParse(_ticker.Value.datetime, out DateTime _datetime))
                        {
                            eventTime = _datetime;
                        }
                        else
                        {
                            eventTime = DateTime.Now;
                        }

                        object GetOrDbNull(object? value) => value != null ? value : DBNull.Value;

                        data.Rows.Add(new object[]
                        {
                            exchangeId,
                            _ticker.Key,
                            currencies[0],
                            currencies[1],
                            eventTime,
                            GetOrDbNull(_ticker.Value.bid),
                            GetOrDbNull(_ticker.Value.ask),
                            GetOrDbNull(_ticker.Value.quoteVolume),
                            updatedTime
                        });
                    }
                }
                logger.Info($"{data.Rows.Count} pairs loaded for exchange {Exchanges[exchangeId].exchange.name}.");
                logger.Info($"{emptyData} empty rows excluded.");
            }
            catch (Exception ex)
            {
                logger.Error(ex, $"Error in method 'GetTickers', exchange {exchangeId}: {ex}");
            }
            return data;
        }
        public async Task<DataTable> GetCurrencies(string exchangeId)
        {
            var data = new DataTable();
            data.Columns.AddRange([
                new DataColumn("exchange_name", typeof(string)),
                new DataColumn("currency", typeof(string)),
                new DataColumn("is_active", typeof(byte)), // Use byte for Nullable(UInt8)
                new DataColumn("networks", typeof(object)), // Use object for Array(Tuple(...))
                new DataColumn("updated_time", typeof(DateTime))
            ]);
            try
            {
                int emptyData = 0;
                DateTime dateTime = DateTime.Now;
                var currencies = await Exchanges[exchangeId].exchange.FetchCurrencies();

                if (currencies.currencies is Dictionary<string, Currency> _currencies)
                {
                    foreach (var _currency in _currencies)
                    {
                        // Map is_active to byte (0/1) or DBNull.Value
                        object isActive = _currency.Value.active == null ? DBNull.Value : (_currency.Value.active == true ? (byte)1 : (byte)0);
                        // Map networks to object[] of object[]

                        object networks = _currency.Value.networks == null ? new object[0] :
                            _currency.Value.networks.Select(p =>
                                new object[]
                                {
                                    p.Key,
                                    p.Value.fee.HasValue ? (object)p.Value.fee.Value : DBNull.Value,
                                    p.Value.withdraw.HasValue ? (object)(p.Value.withdraw.Value ? (byte)1 : (byte)0) : DBNull.Value,
                                    p.Value.active.HasValue ? (object)(p.Value.active.Value ? (byte)1 : (byte)0) : DBNull.Value
                                }).ToArray();

                        data.Rows.Add([
                            exchangeId,
                            _currency.Value.code,
                            isActive,
                            networks,
                            dateTime
                        ]);
                    }
                }

                logger.Info($"{data.Rows.Count} currencies loaded for exchange {Exchanges[exchangeId].exchange.name}.");
                logger.Info($"{emptyData} empty rows excluded.");
            }
            catch (Exception ex)
            {
                logger.Error(ex, $"Error in method 'GetCurrencies', exchange {exchangeId}: {ex}");
            }
            return data;
        }
        public async Task<VerifyOrderBook?> GetOrderBook(string exchangeId, string symbol)
        {
            try
            {
                DateTime dateTime = DateTime.Now;
                var orderBook = await Exchanges[exchangeId].exchange.FetchOrderBook(symbol);
                var asks = orderBook.asks ?? new List<List<double>>();
                var bids = orderBook.bids ?? new List<List<double>>();
                var safeAsks = asks
                    .Where(order => order != null && order.Count > 1)
                    .Select(order => (price: order[0], volume: order[1]))
                    .ToArray();
                var safeBids = bids
                    .Where(order => order != null && order.Count > 1)
                    .Select(order => (price: order[0], volume: order[1]))
                    .ToArray();
                return new VerifyOrderBook()
                {
                    ExchangeName = exchangeId,
                    Symbol = orderBook.symbol,
                    Asks = safeAsks,
                    Bids = safeBids,
                    UpdatedTime = orderBook.datetime is null ? dateTime : Convert.ToDateTime(orderBook.datetime)
                };
            }
            catch (Exception ex)
            {
                logger.Error(ex, $"Error in method 'GetOrderBook', exchange {exchangeId}");
                return null;
            }
        }
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                cts.Cancel();
            }
        }
    }
}
