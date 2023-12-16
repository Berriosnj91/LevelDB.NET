using System.IO;
using System.Text;
using LevelDB.NET;

namespace LevelDbReader
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            using var table = Table.OpenRead(@"E:\state\meta");
            var key = Encoding.UTF8.GetBytes("@layout");
            if (table.TryGetValue(key, out var value)) File.WriteAllBytes("layout.toc", value);

            using var log = File.CreateText("e:\\log.txt");
            foreach (var item in table)
                log.WriteLine($"{Encoding.ASCII.GetString(item.Key)}, {item.Value.Length}");
        }
    }
}