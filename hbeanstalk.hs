import Data.Bits
import Network.Socket
import Network.BSD
import Data.List
import System.IO
import Text.ParserCombinators.Parsec
import Data.Either

data BeanstalkServer = BeanstalkServer {bsHandle :: Handle, bsSocket :: Socket }

connectBeanstalk :: HostName
                 -> String
                 -> IO BeanstalkServer
connectBeanstalk hostname port =
    do -- mostly verbatim from RWH Ch 27 sockets&syslog
       -- Look up the hostname and port.  Either raises an exception
       -- or returns a nonempty list.  First element in that list
       -- is supposed to be the best option.
      addrinfos <- getAddrInfo Nothing (Just hostname) (Just port)
      let serveraddr = head addrinfos
      -- Establish a socket for communication
      sock <- socket (addrFamily serveraddr) Stream defaultProtocol
      -- Mark the socket for keep-alive handling since it may be idle
      -- for long periods of time
      setSocketOption sock KeepAlive 1
      -- We will want to read from this socket, set max msg queue to 5
      --listen sock 5
      -- Connect to server
      connect sock (addrAddress serveraddr)
      -- Make a Handle out of it for convenience
      h <- socketToHandle sock ReadWriteMode
      -- We're going to set buffering to BlockBuffering and then
      -- explicitly call hFlush after each message, below, so that
      -- messages get logged immediately
      hSetBuffering h (BlockBuffering Nothing)
      -- Save off the socket and server address in a handle
      return $ BeanstalkServer h sock

stats :: BeanstalkServer -> IO ()
stats bss =
    do let h = bsHandle bss
       let s = bsSocket bss
       hPutStrLn h "stats\r\n" >> hFlush h
       statHeader <- hGetLine h
       putStrLn statHeader
       let bytes = parseStatsLen statHeader
       putStrLn (show bytes)
--       cont <- hGetContents h
--       putStrLn cont
       (output, bytesread, sa) <- recvFrom s (bytes+2)
       output <- return "blah"
--       hSetBuffering stdout NoBuffering
       putStrLn "test"
       putStrLn output
       hFlush stdout


parseStatsLen :: String -> Int
parseStatsLen input =
        case (parse statsLenParser "StatsLenParser" input) of
          Right len -> read len
          Left err -> 0



-- Parser for first line of stats for data length indicator
statsLenParser :: GenParser Char st String
statsLenParser = char 'O' >> char 'K' >> char ' ' >> many1 digit