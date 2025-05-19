using ccxt;
using ArbitrageScanner.Models;
using System.Data;

namespace ArbitrageScanner.Interfaces
{
    public delegate void putOrderBookHandler(string exchangeId, VerifyOrderBook orderBook, CancellationToken token);
    public interface IExchangeService
    {
        // Order book update event
        event putOrderBookHandler EventOrderBook;
        // Exchanges directory
        Dictionary<string, (decimal fee, Exchange exchange, Queue<string> symbolQueue)> Exchanges { get; }
        // Get market data
        Task<DataTable> GetMarketData(string exchangeId);
        // Get tickers
        Task<DataTable> GetTickers(string exchangeId);
        // Get currencies list
        Task<DataTable> GetCurrencies(string exchangeId);
        // Get order book
        Task<VerifyOrderBook?> GetOrderBook(string exchangeId, string symbol);
    }
}
