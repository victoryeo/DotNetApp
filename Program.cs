using System;
using System.Threading.Tasks;
using Nethereum.Web3;
using Nethereum.Hex.HexTypes;
using Nethereum.BlockchainProcessing.BlockStorage.Repositories;
using Nethereum.RPC.Eth.DTOs;
using System.Numerics;
using System.Threading;
using MySql.Data.MySqlClient;

namespace nethereumapp {
  class Program {

    static void Main(string[] args) {

      databaseprog dbp = new databaseprog();
      dbp.CreateTable();
      dbp.ReadWriteData();

      GetBlockByNumber().Wait();
      //GetBlockNumber().Wait();
      //getTxnCountByNumber().Wait();
      //GetBlockCount().Wait();
      
    }

    static async Task GetBlockByNumber() {
      
      string cs = "server=localhost;database=etherdb;uid=sammy;password=password";
      MySql.Data.MySqlClient.MySqlConnection dbConn = new MySql.Data.MySqlClient.MySqlConnection(cs);
      MySqlCommand cmd;
      string s0;
      dbConn.Open();

      var web3 = new Web3("https://eth-mainnet.g.alchemy.com/v2/CDW6UVk07GwZbMLFX_SyLO415DmR9hco");

      try {
        HexBigInteger hexBigInt = new HexBigInteger(0xB8A1A1);
        var block = await web3.Eth.Blocks.GetBlockWithTransactionsByNumber.SendRequestAsync(hexBigInt);

        Console.WriteLine("Blocks: " + block);
        if (block != null) {
          // write block info to db
          s0 = "INSERT INTO blocks (blockNumber, hash, parentHash, miner, blockReward, gasLimit, gasUsed) VALUES (@blockNumber, @hash, @parentHash, @miner, 5, @gasLimit, @gasUsed);";
          cmd = new MySqlCommand(s0, dbConn);
          cmd.Parameters.AddWithValue("@blockNumber", block.Number);
          cmd.Parameters.AddWithValue("@hash", block.BlockHash);
          cmd.Parameters.AddWithValue("@parentHash", block.ParentHash);
          cmd.Parameters.AddWithValue("@miner", block.Miner);
          cmd.Parameters.AddWithValue("@gasLimit", block.GasLimit);
          cmd.Parameters.AddWithValue("@gasUsed", block.GasUsed);
          cmd.ExecuteNonQuery();
          long blockID = cmd.LastInsertedId;
          Console.WriteLine("blockID: " + blockID);

          foreach (Nethereum.RPC.Eth.DTOs.Transaction e in block.Transactions) {
            Console.WriteLine(
              "  tx hash          : " + e.TransactionHash + "\n"
            + "   nonce           : " + e.Nonce.Value + "\n"
            + "   blockHash       : " + e.BlockHash + "\n"
            + "   blockNumber     : " + e.BlockNumber.Value + "\n"
            + "   transactionIndex: " + e.TransactionIndex.Value + "\n"
            + "   from            : " + e.From + "\n"
            + "   to              : " + e.To + "\n"
            + "   value           : " + Web3.Convert.FromWei(e.Value.Value) + "\n"
            + "   time            : " + block.Timestamp.Value + "\n"
            + "   gasPrice        : " + Web3.Convert.FromWei(e.GasPrice.Value) + "\n"
            + "   gas             : " + e.Gas.Value + "\n"
            + "   input           : " + e.Input
            );
    
            s0 = "INSERT INTO transactions (blockID, hash, fromAr, toAr, valueAr, gas, gasPrice, transactionIndex) VALUES (@blockID, '34E334', 'A478fA', '4006eD', 10, 100, 20, 30);";
            cmd = new MySqlCommand(s0, dbConn);
            cmd.Parameters.AddWithValue("@blockID", blockID);
            cmd.ExecuteNonQuery();
          }
          var txCount = await web3.Eth.Blocks.GetBlockTransactionCountByNumber.SendRequestAsync(hexBigInt);
          Console.WriteLine("Txn Count: " + txCount);
        }

        dbConn.Close();
        Console.WriteLine("Data is written");
      }
      catch {
        Console.WriteLine("Data write error");
      }
    }

    static async Task GetBlockNumber() {
      var web3 = new Web3("https://eth-mainnet.g.alchemy.com/v2/CDW6UVk07GwZbMLFX_SyLO415DmR9hco");
      var latestBlockNumber = await web3.Eth.Blocks.GetBlockNumber.SendRequestAsync();
      Console.WriteLine($"Latest Block Number is: {latestBlockNumber}");
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

      try {
        dbConn.Open();
        s0 = "CREATE DATABASE IF NOT EXISTS `etherdb`;";
        cmd = new MySqlCommand(s0, dbConn);
        cmd.ExecuteNonQuery();
        dbConn.Close();
      }
      catch {
        Console.WriteLine("Constructor error");
      }
    }

    public void CreateTable() {
      string cs = "server=localhost;database=etherdb;uid=sammy;password=password";
      MySql.Data.MySqlClient.MySqlConnection dbConn = new MySql.Data.MySqlClient.MySqlConnection(cs);
      MySqlCommand cmd;
      string s0;

      try {
        dbConn.Open();

        s0 = "DROP TABLE IF EXISTS transactions";
        cmd = new MySqlCommand(s0, dbConn);
        cmd.ExecuteNonQuery();

        s0 = "DROP TABLE IF EXISTS blocks";
        cmd = new MySqlCommand(s0, dbConn);
        cmd.ExecuteNonQuery();

        s0 = "CREATE TABLE `blocks` (`blockID` INT(20) AUTO_INCREMENT, `blockNumber` INT(20), `hash` VARCHAR(66), parentHash VARCHAR(66),`miner` VARCHAR(42),`blockReward` DECIMAL(50,0),`gasLimit` DECIMAL(50,0),`gasUsed` DECIMAL(50,0), PRIMARY KEY(`blockID`));";
        cmd = new MySqlCommand(s0, dbConn);
        cmd.ExecuteNonQuery();

        s0 = "CREATE TABLE `transactions` (`transactionID` INT(20) AUTO_INCREMENT, `blockID` INT(20), `hash` VARCHAR(66), `fromAr` VARCHAR(42), `toAr` VARCHAR(42), `valueAr` DECIMAL(50,0),`gas` DECIMAL(50,0),`gasPrice` DECIMAL(50,0), `transactionIndex` INT(20), PRIMARY KEY(`transactionID`), FOREIGN KEY (`blockID`) REFERENCES `blocks`(`blockID`));";
        cmd = new MySqlCommand(s0, dbConn);
        cmd.ExecuteNonQuery();

        dbConn.Close();
        Console.WriteLine("Table created");
      }
      catch {
        Console.WriteLine("CreateTable error");
      }
    }

    public void ReadWriteData() {
      string cs = "server=localhost;database=etherdb;uid=sammy;password=password";
      MySql.Data.MySqlClient.MySqlConnection dbConn = new MySql.Data.MySqlClient.MySqlConnection(cs);
      MySqlCommand cmd;
      string s0;

      try {
        dbConn.Open();

        s0 = "INSERT INTO blocks (blockNumber, hash, parentHash, miner, blockReward, gasLimit, gasUsed) VALUES (11, '34E334', 'A478fA', '4006eD', 5, 100, 20);";
        cmd = new MySqlCommand(s0, dbConn);
        cmd.ExecuteNonQuery();

        dbConn.Close();
        Console.WriteLine("Data is read write");
      }
      catch {
        Console.WriteLine("ReadWriteData error");
      }
    }
  }
}