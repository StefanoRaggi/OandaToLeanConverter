using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using ICSharpCode.SharpZipLib.GZip;
using ICSharpCode.SharpZipLib.Zip;
using QuantConnect;
using QuantConnect.Configuration;
using QuantConnect.Util;

namespace OandaToLeanConverter
{
    class Program
    {
        static void Main(string[] args)
        {
            var inputFolder = Config.Get("input-folder", "/Oanda/");
            var dataFolder = Config.Get("data-folder", "../../../Lean/Data/");

            var currencies = new HashSet<string>
            {
                "AUD", "CAD", "CHF", "CNY", "CZK", "DKK", "EUR", "GBP", "HKD", "HUF", "INR", "JPY", 
                "MXN", "NOK", "NZD", "PLN", "SAR", "SEK", "SGD", "THB", "TRY", "TWD", "USD", "ZAR"
            };

            var stopwatch = Stopwatch.StartNew();
            var totalTicksProcessed = 0;

            Console.WriteLine("Processing directory: {0}", inputFolder);
            foreach (var fileName in Directory.GetFiles(inputFolder, "oanda_forex_*.txt.gz"))
            {
                var name = Path.GetFileName(fileName) ?? string.Empty;

                //if (string.CompareOrdinal(name.ToLower(), "oanda_forex_usd_zar_2014.txt.gz") < 0)
                //    continue;

                Console.WriteLine("Processing file: {0}", name);

                var tokens = name.Split('_');
                if (tokens.Length != 5)
                {
                    Console.WriteLine("Invalid file name: {0}. Skipped.", name);
                    continue;
                }

                var baseSymbol = tokens[2];
                var quoteSymbol = tokens[3];
                var symbol = baseSymbol + quoteSymbol;

                var securityType = currencies.Contains(baseSymbol) && currencies.Contains(quoteSymbol)
                    ? SecurityType.Forex
                    : SecurityType.Cfd;

                var outputFolder = Path.Combine(dataFolder, securityType.ToString().ToLower(), "oanda", "tick", symbol.ToLower());

                var fileTicksProcessed = 0;
                ZipOutputStream outputStream = null;
                StreamWriter writer = null;
                var lastDate = new DateTime();
                var builder = new StringBuilder();
                var builderLineCount = 0;
                using (var reader = new StreamReader(new GZipInputStream(new FileStream(fileName, FileMode.Open))))
                {
                    string line;
                    while ((line = reader.ReadLine()) != null)
                    {
                        tokens = line.Split(' ');
                        if (tokens.Length != 5) throw new Exception("Invalid file format.");

                        var unixTime = tokens[1];
                        var unixTimeParts = unixTime.Split('.');
                        var unixSeconds = unixTimeParts[0].ToInt32();
                        var unixMillis = unixTimeParts[1].Substring(0, 3).ToInt32();

                        var time = Time.UnixTimeStampToDateTime(unixSeconds).AddMilliseconds(unixMillis);
                        var bid = tokens[2];
                        var ask = tokens[3];
                        var flag = tokens[4];

                        var date = time.Date;
                        if (date != lastDate)
                        {
                            if (writer != null && outputStream != null)
                            {
                                writer.Write(builder.ToString());
                                builder.Clear();
                                builderLineCount = 0;

                                writer.Close();
                                writer.Dispose();
                                outputStream.Close();
                                outputStream.Dispose();
                            }

                            var outputFile = Path.Combine(outputFolder, LeanData.GenerateZipFileName(symbol, securityType, time, Resolution.Tick));
                            Console.WriteLine(outputFile);

                            if (!Directory.Exists(outputFolder)) Directory.CreateDirectory(outputFolder);

                            outputStream = new ZipOutputStream(File.Create(outputFile));
                            writer = new StreamWriter(outputStream);

                            var zipEntry = LeanData.GenerateZipEntryName(symbol, securityType, date, Resolution.Tick, TickType.Quote);
                            outputStream.PutNextEntry(new ZipEntry(zipEntry));

                            lastDate = date;
                        }

                        if (writer != null && builderLineCount > 1000)
                        {
                            writer.Write(builder.ToString());
                            builder.Clear();
                            builderLineCount = 0;
                        }

                        builder.AppendFormat("{0},{1},{2},{3}{4}", time.TimeOfDay.TotalMilliseconds, bid, ask, flag, Environment.NewLine);
                        builderLineCount++;

                        fileTicksProcessed++;
                        totalTicksProcessed++;
                    }

                    if (writer != null && outputStream != null)
                    {
                        writer.Write(builder.ToString());
                        builder.Clear();
                        builderLineCount = 0;

                        writer.Close();
                        writer.Dispose();
                        outputStream.Close();
                        outputStream.Dispose();
                    }
                }

                Console.WriteLine("File ticks processed: {0}", fileTicksProcessed);
            }

            stopwatch.Stop();
            Console.WriteLine("Total ticks processed: {0}, elapsed time: {1} [{2:N0}K ticks/sec.]", 
                totalTicksProcessed, stopwatch.Elapsed, totalTicksProcessed / stopwatch.Elapsed.TotalSeconds / 1000);
        }
    }
}
