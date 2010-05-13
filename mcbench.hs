module Main where

import Control.Monad
import System
import System.IO
import qualified Network.Starling as Starling
import qualified Network.Fancy as Fancy
import Control.Concurrent
import qualified Data.ByteString.Lazy.Char8 as C
import qualified Data.ByteString.Lazy as B
import Control.Concurrent.STM
import Data.Time.Clock.POSIX
import qualified Control.Exception as Ex

main = do args <- getArgs
          case args of
            [host, port', threads'] ->
                run host (read port') (read threads')
            _ ->
                do pn <- getProgName
                   putStrLn $ "Usage: " ++ pn ++ " <host> <port> <#threads>"

data Stats = Stats { statsRequests :: Integer,
                     statsLatency :: POSIXTime,
                     statsBytes :: Integer
                   }

run :: String -> Int -> Int -> IO ()
run host port threads
    = do tStats <- atomically $
                   newTVar $ Stats 0 0 0
         let start w = do handle <- Fancy.connectStream (Fancy.IP host port)
                          hSetBuffering handle NoBuffering
                          conn <- Starling.open handle
                          worker w 0 conn tStats
         threads <- forM [1..threads] $ \w ->
                    forkIO $ catch (start w) $
                               \e ->
                                   do putStrLn $ show e
                                      exitWith $ ExitFailure 1
         statsLoop tStats

statsLoop :: TVar Stats -> IO ()
statsLoop tStats
    = getPOSIXTime >>= statsLoop' tStats
statsLoop' tStats t1
    = do threadDelay 1000000
         Stats req lat bytes <- atomically $ do
                                  stats <- readTVar tStats
                                  writeTVar tStats $ Stats 0 0 0
                                  return stats
         t2 <- getPOSIXTime
         let d = t2 - t1
         putStrLn $ "Stats: " ++ show (req // d) ++ " req/s\t" ++
                  show ((lat * 1000000) // req) ++ " us avg latency\t" ++
                  formatRate (bytes // d)
         statsLoop' tStats t2
    where (//) :: (Real q, Real d, Integral a) => q -> d -> a
          _ // 0 = -1
          q // d = truncate (toRational q / toRational d)
          formatRate :: Integer -> String
          formatRate = let formats = ["K", "M", "G", "T"]
                           format :: [String] -> Integer -> String
                           format [f] r = show r ++ " " ++ f ++ "B/s"
                           format (f:fs) r
                               | r >= 1024 * 12 = format fs (r `div` 1024)
                               | otherwise = format [f] r
                       in format formats

worker :: Int -> Int -> Starling.Connection -> TVar Stats -> IO ()
worker w i conn tStats
    = do t1 <- getPOSIXTime
         Starling.set conn (C.pack $ "test-" ++ show w ++ "-" ++ show i) body
         t2 <- getPOSIXTime
         atomically $ do
           stats <- readTVar tStats
           writeTVar tStats $
                     stats { statsRequests = statsRequests stats + 1,
                             statsLatency = statsLatency stats + t2 - t1,
                             statsBytes = statsBytes stats + fromIntegral (C.length body)
                           }
         worker w (i + 1) conn tStats

body = B.replicate (32 * 1024) 255
