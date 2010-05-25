module Main where
import Network.Beanstalk
import Data.Bits
import Network.Socket
import Network.BSD
import Data.List
import System.IO
import Text.ParserCombinators.Parsec
import Data.Yaml.Syck
import Data.Typeable
import qualified Data.Map as M
import qualified Control.Exception as E
import Data.Maybe
import Control.Monad

-- Testing
main = do bs <- connectBeanstalk "localhost" "8887"
          printServerStats bs
          -- If server has no jobs, this should wait for 1 second and then timeout
          e <- E.tryJust (guard . isTimedOutException) (reserveJobWithTimeout bs 1)
          putStrLn (show e)
          mapM_ (\x -> putJob bs 1 0 500 ("hello "++(show x))) [1..10]
          job <- putJob bs 1 0 500 "hello"
          rjob <- reserveJob bs
          putStrLn $ "Found job with ID: " ++ (show (job_id rjob)) ++ " and body: " ++ (job_body rjob)
          rjob <- reserveJob bs
          putStrLn $ "Found job with ID: " ++ (show (job_id rjob)) ++ " and body: " ++ (job_body rjob)
          rjob <- reserveJob bs
          putStrLn $ "Found job with ID: " ++ (show (job_id rjob)) ++ " and body: " ++ (job_body rjob)
          -- Do a delete with guard to protect against exceptions.  This will succeed.
          e <- E.tryJust (guard . isNotFoundException) (deleteJob bs (job_id rjob))
          putStrLn (show e)
          -- But this will fail with a NOT_FOUND error.
          e <- E.tryJust (guard . isNotFoundException) (deleteJob bs 9999999)
          putStrLn (show e)
          useTube bs "hbeanstalk"
          watchCount <- watchTube bs "default"
          watchCount <- watchTube bs "hbeanstalk"
          putStrLn ("Now watching "++(show watchCount)++" tubes")
          job <- putJob bs 1 500 500 "hello"
          rjob <- reserveJob bs
          putStrLn ("About to try to bury job "++(show (job_id rjob)))
          buryJob bs (job_id rjob) 1
          printServerStats bs
          rjob <- reserveJob bs
          rjob <- reserveJobWithTimeout bs 5
          releaseJob bs (job_id rjob) 1 1
          putStrLn "exiting"