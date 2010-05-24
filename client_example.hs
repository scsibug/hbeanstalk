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
          job <- putJob bs 1 0 500 "hello"
          --printServerStats bs
          putStrLn "exiting"