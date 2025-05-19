namespace ArbitrageScanner.Models
{
    // Состояние сделок
    public enum ProfitTradesState
    {
        New = 0,            // Новая сделка
        Checking = 1,       // Сделка проверяется
        Checked = 2,        // Проверена
        Unconfirmed = 3,    // Неподтверждена    
        ReChecking = 4,     // Перепроверка
        Expired = 5         // Истекла
    }
    public struct PairStruct : IEquatable<PairStruct>
    {
        public string Symbol { get; }
        public string BuyExchange { get; }
        public string SellExchange { get; }
        public PairStruct(string symbol, string buyExchange, string sellExchange)
        {
            Symbol = symbol;
            BuyExchange = buyExchange;
            SellExchange = sellExchange;
        }
        public override bool Equals(object obj) => obj is PairStruct other && Equals(other);
        public bool Equals(PairStruct other)
        {
            return Symbol == other.Symbol &&
                   BuyExchange == other.BuyExchange &&
                   SellExchange == other.SellExchange;
        }
        public override int GetHashCode()
        {
            return HashCode.Combine(Symbol, BuyExchange, SellExchange);
        }
        public override string ToString() => $"{Symbol} ({BuyExchange} → {SellExchange})";
    }
    public class Pair
    {
        public string Symbol { get; set; } // Торговая пара
        public string BuyExchange { get; set; } // Биржа покупки
        public string SellExchange { get; set; } // Биржа продажи
        public string GetName()
        {
            return $"{Symbol}: {BuyExchange} => {SellExchange}";
        }
    }
    public class ProfitTrade
    {
        public Pair Pair { get; set; } // Торговая пара
        public double BuyPrice { get; set; } // Цена покупки
        public double SellPrice { get; set; } // Цена продажи
        public DateTime EventTime { get; set; } // Дата события
        public double PrcProfit { get; set; } // Процент профита
        public Network[] Networks { get; set; } = []; // Общие сети бирж
        public bool? HasFuture { get; set; } // Наличие рынка фьючерсов для торговой пары
        public bool? HasSwap { get; set; } // Наличие рынка бессрочных фьючерсов для торговой пары
        public double PrcRealProfit { get; set; } // Процент профита после проверки
        public ProfitTradesState State { get; set; } = ProfitTradesState.New; // Состояние
        public DateTime? VerifyTime { get; set; } // Дата начала проверки
        public VerifyOrderBook? BuyOrderBook { get; set; }
        public VerifyOrderBook? SellOrderBook { get; set; }
        public ProfitTrade Clone() => new()
        {
            Pair = new()
            {
                Symbol = Pair.Symbol,
                BuyExchange = Pair.BuyExchange,
                SellExchange = Pair.SellExchange
            },
            BuyPrice = BuyPrice,
            SellPrice = SellPrice,
            EventTime = EventTime,
            PrcProfit = PrcProfit,
            Networks = [.. Networks],
            HasFuture = HasFuture,
            HasSwap = HasSwap,
            State = State,
            VerifyTime = VerifyTime,
            BuyOrderBook = BuyOrderBook,
            SellOrderBook = SellOrderBook
        };
    }
    public class Network
    {
        public string Name { get; set; } // Название сети
        public double? BuyFee { get; set; } // Коммисия сети на бирже покупки
        public bool? BuyWithdraw { get; set; } // Возможность вывода с биржы покупки
        public bool? BuyActive { get; set; } // Активность сети на бирже покупки
        public bool? SellActive { get; set; } // Активность сети на бирже продажи
    }
    public class VerifyOrderBook
    {
        public string ExchangeName { get; set; } // Идентификатор биржи
        public string Symbol { get; set; } // Коммисия сети на бирже покупки
        public (double price, double volume)[] Asks { get; set; } = [];// Возможность вывода с биржы покупки
        public (double price, double volume)[] Bids { get; set; } = [];// Активность сети на бирже покупки
        public DateTime UpdatedTime { get; set; } // Активность сети на бирже продажи
    }

    public class SpotSpotDeal
    {
        public DateTime EventTime { get; set; }
        public string Symbol { get; set; }
        public string NetworkName { get; set; }
        public string BuyExchange { get; set; }
        public string SellExchange { get; set; }
        public double SelectedBuyFee { get; set; }
        public int BuyOrderCount { get; set; }
        public int SellOrderCount { get; set; }
        public double TotalBuyCost { get; set; }
        public double TotalSellRevenue { get; set; }
        public double AvgBuyPrice { get; set; }
        public double AvgSellPrice { get; set; }
        public double MinBuyPrice { get; set; }
        public double MaxSellPrice { get; set; }
        public double MinBuyVolume { get; set; }
        public double MaxSellVolume { get; set; }
        public double MaxProfitUSDT { get; set; }
        public double ProfitPercentage { get; set; }
        public double MinProfitUSDT { get; set; }
        public double MinProfitPercentage { get; set; }
        public double Deposit { get; set; }
        public double CustomTotalBuyCost { get; set; }
        public double CustomProfitUSDT { get; set; }
        public double CustomProfitPercentage { get; set; }
    }
}
