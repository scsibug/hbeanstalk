{-# LANGUAGE DeriveDataTypeable #-}

module Network.Beanstalk where
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
type BeanstalkServer = Socket

data Job = Job {job_id :: Int,
                job_body :: String}


-- Exceptions from Beanstalk
data BeanstalkException = NotFoundException | OutOfMemoryException |
                          InternalErrorException | DrainingException |
                          BadFormatException | UnknownCommandException |
                          JobTooBigException | ExpectedCRLFException |
                          DeadlineSoonException | TimedOutException |
                          NotIgnoredException
    deriving (Show, Typeable)
instance E.Exception BeanstalkException

isNotFoundException :: BeanstalkException -> Bool
isNotFoundException be = case be of
                              NotFoundException -> True
                              _ -> False
isBadFormatException :: BeanstalkException -> Bool
isBadFormatException be = case be of
                                 BadFormatException -> True
                                 _ -> False

connectBeanstalk :: HostName
                 -> String
                 -> IO BeanstalkServer
connectBeanstalk hostname port =
    do addrinfos <- getAddrInfo Nothing (Just hostname) (Just port)
       let serveraddr = head addrinfos
       -- Establish a socket for communication
       sock <- socket (addrFamily serveraddr) Stream defaultProtocol
       -- Mark the socket for keep-alive handling since it may be idle
       -- for long periods of time
       setSocketOption sock KeepAlive 1
       -- Connect to server
       connect sock (addrAddress serveraddr)
       return sock

-- put <pri> <delay> <ttr> <bytes>\r\n
-- <data>\r\n
putJob :: BeanstalkServer -> Int -> Int -> Int -> String -> IO String
putJob s priority delay ttr job_body =
    do let job_size = length job_body
       send s ("put " ++
               (show priority) ++ " " ++
               (show delay) ++ " " ++
               (show ttr) ++ " " ++
               (show job_size) ++ "\r\n")
       send s (job_body ++ "\r\n")
       status <- readLine s
       checkForBeanstalkErrors status
       putStrLn status
       return "3"

reserveJob :: BeanstalkServer -> IO Job
reserveJob s =
    do send s "reserve\r\n"
       response <- readLine s
       putStrLn response
       let (jobid, bytes) = parseReserve response
       (jobContent, bytesRead) <- recvLen s (bytes+2)
       return (Job (read jobid) jobContent)

deleteJob :: BeanstalkServer -> Int -> IO ()
deleteJob s jobid =
    do send s ("delete "++(show jobid)++"\r\n")
       response <- readLine s
       checkForBeanstalkErrors response
       putStrLn response

checkForBeanstalkErrors :: String -> IO ()
checkForBeanstalkErrors input =
    do eop OutOfMemoryException "OUT_OF_MEMORY\r\n"
       eop InternalErrorException "INTERNAL_ERROR\r\n"
       eop DrainingException "DRAINING\r\n"
       eop BadFormatException "BAD_FORMAT\r\n"
       eop UnknownCommandException "UNKNOWN_COMMAND\r\n"
       eop NotFoundException "NOT_FOUND\r\n"
       eop JobTooBigException "JOB_TOO_BIG\r\n"
       eop ExpectedCRLFException "EXPECTED_CRLF\r\n"
       eop DeadlineSoonException "DEADLINE_SOON\r\n"
       eop TimedOutException "TIMED_OUT\r\n"
       eop NotIgnoredException "NOT_IGNORED\r\n"
       where eop e s = exceptionOnParse e (parse (string s) "errorParser" input)

-- When an error is successfully parsed, throw the given exception.
exceptionOnParse :: BeanstalkException -> Either a b -> IO ()
exceptionOnParse e x = case x of
                    Right _ -> E.throwIO e
                    Left _ -> return ()

useTube :: BeanstalkServer -> String -> IO ()
useTube s name =
    do send s ("use "++name++"\r\n");
       response <- readLine s
       putStrLn response

getServerStats :: BeanstalkServer -> IO (M.Map String String)
getServerStats s =
    do send s "stats\r\n"
       statHeader <- readLine s
       let bytes = parseStatsLen statHeader
       (statContent, bytesRead) <- recvLen s (bytes+2)
       yamlN <- parseYaml statContent
       return $ yamlMapToHMap yamlN

printServerStats :: BeanstalkServer -> IO ()
printServerStats s =
    do stats <- getServerStats s
       let kv = M.assocs stats
       mapM_ (\(k,v) -> putStrLn (k ++ " => " ++ v)) kv

yamlMapToHMap :: YamlNode -> M.Map String String
yamlMapToHMap y = M.fromList elems where
    emap = (n_elem y)
    EMap maplist = emap -- [(YamlNode,YamlNode)]
    yelems = map (\(x,y) -> (n_elem x, n_elem y))  maplist
    elems = map (\(EStr x, EStr y) -> (unpackBuf x, unpackBuf y)) yelems

-- Read a single character from socket without handling errors
readChar :: Socket -> IO Char
readChar s = recv s 1 >>= return . head

-- Read up to and including a newline.  Any errors result in a string
-- starting with "Error: "
readLine :: Socket -> IO String
readLine s =
    catch readLine' (\err -> return ("Error: " ++ show err))
        where
          readLine' = do c <- readChar s
                         if c == '\n'
                           then return (c:[])
                           else do l <- readLine s
                                   return (c:l)

-- Get Job ID and size
parseReserve :: String -> (String,Int)
parseReserve input =
    case (parse reservedParser "ReservedParser" input) of
      Right (x,y) -> (x, read y)
      Left err -> ("",0)

reservedParser :: GenParser Char st (String,String)
reservedParser = do string "RESERVED"
                    char ' '
                    x <- many1 digit
                    char ' '
                    y <- many1 digit
                    return (x,y)

parseStatsLen :: String -> Int
parseStatsLen input =
        case (parse statsLenParser "StatsLenParser" input) of
          Right len -> read len
          Left err -> 0

-- Parser for first line of stats for data length indicator
statsLenParser :: GenParser Char st String
statsLenParser = string "OK " >> many1 digit