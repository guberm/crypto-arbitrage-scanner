using ArbitrageScanner.Models;
using System.Data;

namespace ArbitrageScanner.Interfaces
{
    public interface IDBService
    {
        Task<bool> Export(DataTable data, string tableName, CancellationToken token);
        Task<bool> RemoveItem(ProfitTrade trade, string tableName);
        Task<bool> TruncateTables();
        Task<ProfitTrade[]> GetProfitTrades(long minVolume, (int minValue, int maxValue) prcProfitRange, string[] whitelist, int lagUpdated);
    }
}
