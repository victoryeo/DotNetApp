using System;
using System.Threading.Tasks;
using Nethereum.Web3;
using Nethereum.Hex.HexTypes;
using Nethereum.BlockchainProcessing.BlockStorage.Repositories;
using Nethereum.RPC.Eth.DTOs;
using System.Numerics;
using System.Threading;
using MySql.Data.MySqlClient;

namespace nethereumapp
{
  class Program
  {

    static void Main(string[] args)
    {
      GetBlockByNumber().Wait();
      //GetBlockNumber().Wait();
      //getTxnCountByNumber().Wait();
      //GetBlockCount().Wait();
      
      databaseprog dbp = new databaseprog();
      dbp.CreateTable();

    }

    static async Task GetBlockNumber()
    {
      var web3 = new Web3("https://eth-mainnet.g.alchemy.com/v2/CDW6UVk07GwZbMLFX_SyLO415DmR9hco");
      var latestBlockNumber = await web3.Eth.Blocks.GetBlockNumber.SendRequestAsync();
      Console.WriteLine($"Latest Block Number is: {latestBlockNumber}");
    }

    static async Task GetBlockByNumber()
    {
      var web3 = new Web3("https://eth-mainnet.g.alchemy.com/v2/CDW6UVk07GwZbMLFX_SyLO415DmR9hco");

      HexBigInteger hexBigInt = new HexBigInteger(0xB8A1A1);
      var block = await web3.Eth.Blocks.GetBlockWithTransactionsByNumber.SendRequestAsync(hexBigInt);

      Console.WriteLine("Blocks: " + block);
      if (block != null) {
          foreach (Nethereum.RPC.Eth.DTOs.Transaction e in block.Transactions) {
            Console.WriteLine(
              "  tx hash          : " + e.TransactionHash + "\n"
            + "   nonce           : " + e.Nonce.Value + "\n"
            + "   blockHash       : " + e.BlockHash + "\n"
            + "   from            : " + e.From + "\n"
            + "   to              : " + e.To + "\n"
            + "   blockNumber     : " + e.BlockNumber.Value + "\n"
            );
          }
          var txCount = await web3.Eth.Blocks.GetBlockTransactionCountByNumber.SendRequestAsync(hexBigInt);
          Console.WriteLine("Txn Count: " + txCount);
      }
    }

    static async Task getTxnCountByNumber() {
      var web3 = new Web3("https://eth-mainnet.g.alchemy.com/v2/CDW6UVk07GwZbMLFX_SyLO415DmR9hco");

      var blkLatest = BlockParameter.CreatePending();
      var TxnCount = await web3.Eth.Blocks.GetBlockTransactionCountByNumber.SendRequestAsync(blkLatest);
      Console.WriteLine("Pending block: " + blkLatest);
      Console.WriteLine($" Txn Count {TxnCount}");
    }

    static async Task GetBlockCount() {
      var web3 = new Web3("https://eth-mainnet.g.alchemy.com/v2/CDW6UVk07GwZbMLFX_SyLO415DmR9hco");

      var context = new InMemoryBlockchainStorageRepositoryContext();
      var repoFactory = new InMemoryBlockchainStoreRepositoryFactory(context);
      var processor = web3.Processing.Blocks.CreateBlockStorageProcessor(
        repoFactory
      );
      var cancellationToken = new CancellationToken();

      //crawl the required block range
      await processor.ExecuteAsync(
      toBlockNumber: new BigInteger(2),
      cancellationToken: cancellationToken,
      startAtBlockNumberIfNotProcessed: new BigInteger(1));

      Console.WriteLine($"Block Count {context.Blocks.Count}");
    }
  }

  public class databaseprog {
    public databaseprog() {
      string cs = "server=localhost;database=etherdb;uid=sammy;password=password";
      MySql.Data.MySqlClient.MySqlConnection dbConn = new MySql.Data.MySqlClient.MySqlConnection(cs);

      using var con = new MySqlConnection(cs);
      con.Open();
      Console.WriteLine($"MySQL version : {con.ServerVersion}");

      MySqlCommand cmd;
      string s0;

      try
      {
        dbConn.Open();
        s0 = "CREATE DATABASE IF NOT EXISTS `etherdb`;";
        cmd = new MySqlCommand(s0, dbConn);
        cmd.ExecuteNonQuery();
        dbConn.Close();
      }
      catch
      {
        Console.WriteLine("constructor Error happens");
      }
    }

    public void CreateTable()
    {
      string cs = "server=localhost;database=etherdb;uid=sammy;password=password";
      MySql.Data.MySqlClient.MySqlConnection dbConn = new MySql.Data.MySqlClient.MySqlConnection(cs);
      MySqlCommand cmd;
      string s0;

      try
      {
        dbConn.Open();
        s0 = "DROP TABLE IF EXISTS blocks";
        cmd = new MySqlCommand(s0, dbConn);
        cmd.ExecuteNonQuery();
        s0 = "CREATE TABLE `blocks` (`blockId` INT AUTO_INCREMENT, `blockNumber` INT(20), `hash` VARCHAR(66), `parentHash` VARCHAR(66), parentHash VARCHAR(66),`miner` VARCHAR(42),`blockReward` DECIMAL(50,0),`gasLimit` DECIMAL(50,0),`gasUsed` DECIMAL(50,0), PRIMARY KEY(`blockId`));";
        cmd.ExecuteNonQuery();
        dbConn.Close();
        Console.WriteLine("Table created");
      }
      catch
      {
        Console.WriteLine("CreateTable Error happens");
      }
    }
  }
}