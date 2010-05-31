{-# LANGUAGE DeriveDataTypeable #-}
-----------------------------------------------------------------------------
-- |
-- Module      :  Network.Beanstalk
-- Copyright   :  (c) Greg Heartsfield 2010
-- License     :  BSD3
--
-- Client API to beanstalkd work queue.
-----------------------------------------------------------------------------

module Network.Beanstalk (
  -- * Connecting and Disconnecting
  connectBeanstalk, disconnectBeanstalk,
  -- * Beanstalk Job Commands
  putJob, releaseJob, reserveJob, reserveJobWithTimeout, deleteJob, buryJob,
  peekJob, peekReadyJob, peekDelayedJob, peekBuriedJob, kickJobs,
  -- * Beanstalk Tube Commands
  useTube, watchTube, ignoreTube, listTubes, listTubesWatched, listTubeUsed,
  -- * Beanstalk Stats Commands
  statsJob, statsTube, statsServer, jobCountWithState,
  -- * Pretty-Printing Stats and Lists
  printStats, printList,
  -- * Exception Predicates
  isNotFoundException, isBadFormatException, isTimedOutException,
  isOutOfMemoryException, isInternalErrorException, isJobTooBigException,
  isDeadlineSoonException, isNotIgnoredException,
  -- * Data Types
  Job(..), BeanstalkServer, JobState(..), BeanstalkException(..)
  ) where

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
import Control.Concurrent.MVar

-- | Beanstalk Server, wrapped in an 'MVar' for synchronizing access
--   to the server socket.  As many of these can be created as are
--   needed, but jobs are associated to a single server session and
--   must be released/deleted with the same session that reserved
--   them.
type BeanstalkServer = MVar Socket

-- | Information essential to performing a job and operating on it.
data Job =
    Job { -- | Job numeric identifier
          job_id :: Int,
          -- | Job body
          job_body :: String}
           deriving (Show, Read, Eq)

-- | States describing the lifecycle of a job.
data JobState = -- | Ready, retrievable with 'reserveJob'
                READY |
                -- | Reserved by a worker
                RESERVED |
                -- | Delayed, waiting to be put in ready queue
                DELAYED |
                -- | Buried, can be resurrected with 'kickJobs'
                BURIED
              deriving (Show, Read, Eq)

-- | Exceptions generated from the beanstalkd server
data BeanstalkException =
    -- | Job does not exist, or is not reserved by this client.
    NotFoundException |
    -- | The server did not have enough memory available to create the job.
    OutOfMemoryException |
    -- | The server detected an internal error.  If this happens, please report to
    --   <http://groups.google.com/group/beanstalk-talk>.
    InternalErrorException |
    -- | The server is in drain mode, and is not accepting new jobs.
    DrainingException |
    -- | Client sent a command that was not understood.  May indicate
    --   a bad argument list or other format violation.
    BadFormatException |
    -- | The server did not recognize a command.  Should never occur,
    --   this is either a bug in the hbeanstalk library or an
    --   incompatible server version.
    UnknownCommandException |
    -- | A 'putJob' call included a body larger than the server's
    --   @max-job-size@ setting allows.
    JobTooBigException |
    -- | This library failed to terminate a job body with a CR-LF
    --   terminator.  Should never occur, if it does it is a bug in
    --   hbeanstalk.
    ExpectedCRLFException |
    -- | Not strictly an error condition, this indicates a job this
    --   client has reserved is about to expire.  See
    --   <http://groups.google.com/group/beanstalk-talk/browse_thread/thread/232d0cac5bebe30f>
    --   for a detailed explanation.
    DeadlineSoonException |
    -- | Timeout for @reserveJobWithTimeout@ expired before a job became available.
    TimedOutException |
    -- | Client attempted to ignore the only tube in its watch list (clients
    --   must always watch one or more tubes).
    NotIgnoredException
    deriving (Show, Typeable, Eq)

instance E.Exception BeanstalkException

-- | Predicate to detect 'NotFoundException'
isNotFoundException :: BeanstalkException -> Bool
isNotFoundException = (==) NotFoundException

-- | Predicate to detect 'OutOfMemoryException'
isOutOfMemoryException :: BeanstalkException -> Bool
isOutOfMemoryException = (==) OutOfMemoryException

-- | Predicate to detect 'InternalErrorException'
isInternalErrorException :: BeanstalkException -> Bool
isInternalErrorException = (==) InternalErrorException

-- | Predicate to detect 'DrainingException'
isDrainingException :: BeanstalkException -> Bool
isDrainingException = (==) DrainingException

-- | Predicate to detect 'BadFormatException'
isBadFormatException :: BeanstalkException -> Bool
isBadFormatException = (==) BadFormatException

-- | Predicate to detect 'JobTooBigException'
isJobTooBigException :: BeanstalkException -> Bool
isJobTooBigException = (==) JobTooBigException

-- | Predicate to detect 'DeadlineSoonException'
isDeadlineSoonException :: BeanstalkException -> Bool
isDeadlineSoonException = (==) DeadlineSoonException

-- | Predicate to detect 'TimedOutException'
isTimedOutException :: BeanstalkException -> Bool
isTimedOutException = (==) TimedOutException

-- | Predicate to detect 'NotIgnoredException'
isNotIgnoredException :: BeanstalkException -> Bool
isNotIgnoredException = (==) NotIgnoredException

-- | Connect to a beanstalkd server.
connectBeanstalk :: HostName -- ^ Hostname of beanstalkd server
                 -> String -- ^ Port number of server
                 -> IO BeanstalkServer -- ^ Server object for commands to operate on
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
       bs <- newMVar sock
       return bs

-- | Disconnect from a beanstalkd server.  Any jobs reserved from this connection will be released
disconnectBeanstalk :: BeanstalkServer -- ^ Beanstalk server
                    -> IO ()
disconnectBeanstalk bs = withMVar bs task
    where task s = sClose s

-- | Put a new job on the current tube that was selected with useTube.
-- Specify numeric priority, delay before becoming active, a limit
-- on the time-to-run, and a job body.  Returns job state and ID.
putJob :: BeanstalkServer -- ^ Beanstalk server
       -> Int -- ^ Job priority: an integer less than 2**32. Jobs with smaller
             --   priorities are scheduled before jobs with
             --   larger priorities.  The most urgent priority is 0; the
             --   least urgent priority is 4294967295.
       -> Int -- ^ Number of seconds to delay putting the job into the
             --   ready queue.  Until that time expires, the job will
             --   have a state of 'DELAYED'.
       -> Int -- ^ Maximum time-to-run in seconds for a reserved job.
             --   This timer starts when a job is reserved.  If it
             --   expires without a 'deleteJob', 'releaseJob',
             --   'buryJob', or 'touchJob' command being run, the job
             --   will be placed back on the ready queue.  The minimum
             --   value is 1.
       -> String -- ^ Job body.
       -> IO (JobState, Int) -- ^ State of the newly created job and its ID
putJob bs priority delay ttr job_body = withMVar bs task
    where task s =
              do let job_size = length job_body
                 send s ("put " ++
                         (show priority) ++ " " ++
                         (show delay) ++ " " ++
                         (show ttr) ++ " " ++
                         (show job_size) ++ "\r\n")
                 send s (job_body ++ "\r\n")
                 response <- readLine s
                 checkForBeanstalkErrors response
                 let (state, jobid) = parsePut response
                 return (state, jobid)

-- | Reserve a new job from the watched tube list, blocking until one
--   becomes available. 'DeadlineSoonException' may be thrown if a job
--   reserved by the same client is about to expire.
reserveJob :: BeanstalkServer -- ^ Beanstalk server
           -> IO Job -- ^ Job reserved by this client
reserveJob bs = withMVar bs task
    where task s =
              do send s "reserve\r\n"
                 response <- readLine s
                 checkForBeanstalkErrors response
                 let (jobid, bytes) = parseReserve response
                 (jobContent, bytesRead) <- recvLen s (bytes)
                 recv s 2 -- Ending CRLF
                 return (Job (read jobid) jobContent)

-- | Reserve a job from the watched tube list, blocking for the specified number
-- of seconds or until a job is returned.  If no jobs are found before the
-- timeout value, a TimedOutException will be thrown.  If another reserved job
-- is about to exceed its time-to-run, a DeadlineSoonException will be thrown.
reserveJobWithTimeout :: BeanstalkServer -- ^ Beanstalk server
                      -> Int -- ^ Time in seconds to wait for a job
                            --   to become available.  Once this time
                            --   passes, a 'TimedOutException' will be
                            --   thrown.
                      -> IO Job -- ^ Job reserved by this client
reserveJobWithTimeout bs seconds = withMVar bs task
    where task s =
              do send s ("reserve-with-timeout "++(show seconds)++"\r\n")
                 response <- readLine s
                 checkForBeanstalkErrors response
                 let (jobid, bytes) = parseReserve response
                 (jobContent, bytesRead) <- recvLen s (bytes)
                 recv s 2 -- Ending CRLF
                 return (Job (read jobid) jobContent)

-- | Delete a job to indicate that it has been completed.  If the job
--   does not exist, was not reserved by this client, or is not in the
--   'READY' or 'BURIED' state, a 'NotFoundException' will be thrown.
deleteJob :: BeanstalkServer -- ^ Beanstalk server
          -> Int -- ^ ID of the job to delete
          -> IO ()
deleteJob bs jobid = withMVar bs task
    where task s =
              do send s ("delete "++(show jobid)++"\r\n")
                 response <- readLine s
                 checkForBeanstalkErrors response

-- | Indicate that a job should be released back to the tube for another consumer.
releaseJob :: BeanstalkServer -- ^ Beanstalk server
           -> Int -- ^ ID of the job to release
           -> Int -- ^ New priority to assign the job
           -> Int -- ^ Delay before the job is placed on the ready queue
           -> IO ()
releaseJob bs jobid priority delay = withMVar bs task
    where task s =
              do send s ("release " ++
                         (show jobid) ++ " " ++
                         (show priority) ++ " " ++
                         (show delay) ++ "\r\n")
                 response <- readLine s
                 checkForBeanstalkErrors response

-- | Bury a job so that it cannot be reserved.
buryJob :: BeanstalkServer -- ^ Beanstalk server
        -> Int -- ^ ID of the job to bury
        -> Int -- ^ New priority to assign the job
        -> IO ()
buryJob bs jobid pri = withMVar bs task
    where task s =
              do send s ("bury "++(show jobid)++" "++(show pri)++"\r\n")
                 response <- readLine s
                 checkForBeanstalkErrors response

checkForBeanstalkErrors :: String -- ^ beanstalkd server response
                        -> IO ()
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

-- | When an error is successfully parsed, throw the given exception.
exceptionOnParse :: BeanstalkException -> Either a b -> IO ()
exceptionOnParse e x = case x of
                    Right _ -> E.throwIO e
                    Left _ -> return ()

-- | Assign a tube for new jobs created with put command.  If the tube
--   does not already exist, it will be created.  Initially, all
--   sessions will use the tube named \"default\".
useTube :: BeanstalkServer -- ^ Beanstalk server
        -> String -- ^ Name of tube to watch
        -> IO ()
useTube bs name = withMVar bs task
    where task s =
              do send s ("use "++name++"\r\n");
                 response <- readLine s
                 checkForBeanstalkErrors response

-- | Add a named tube to the watch list, those tubes which
--   'reserveJob' will request jobs from.
watchTube :: BeanstalkServer -- ^ Beanstalk server
          -> String -- ^ Name of tube to watch
          -> IO Int -- ^ Number of tubes currently being watched
watchTube bs name = withMVar bs task
    where task s =
              do send s ("watch "++name++"\r\n");
                 response <- readLine s
                 checkForBeanstalkErrors response
                 return $ parseWatching response

-- | Removes the named tube to watch list.  If the tube being ignored
--   is the only one currently being watched, a 'NotIgnoredException'
--   is thrown.
ignoreTube :: BeanstalkServer -- ^ Beanstalk server
           -> String -- ^ Name of tube to ignore
           -> IO Int -- ^ Number of tubes currently being watched
ignoreTube bs name = withMVar bs task
    where task s =
              do send s ("ignore "++name++"\r\n");
                 response <- readLine s
                 checkForBeanstalkErrors response
                 return $ parseWatching response

-- | Inspect a specific job in the system.
peekJob :: BeanstalkServer -- ^ Beanstalk server
        -> Int -- ^ ID of job to get information about
        -> IO Job -- ^ Job definition
peekJob bs jobid = genericPeek bs ("peek "++(show jobid))

-- | Inspect the next ready job on the currently used tube.
peekReadyJob :: BeanstalkServer -- ^ Beanstalk server
             -> IO Job -- ^ Job definition
peekReadyJob bs = genericPeek bs "peek-ready"

-- | Inspect the delayed job with shortest delay remaining on the currently used tube.
peekDelayedJob :: BeanstalkServer -- ^ Beanstalk server
               -> IO Job -- ^ Job definition
peekDelayedJob bs = genericPeek bs "peek-delayed"

-- | Inspect the next buried job on the currently used tube.
peekBuriedJob :: BeanstalkServer -- ^ Beanstalk server
              -> IO Job -- ^ Job definition
peekBuriedJob bs = genericPeek bs "peek-buried"

-- Essence of the peek command.  Variations (peek, peek-ready,
-- peek-delayed, peek-buried) just provide the command string, while
-- this function actually executes it and parses the results.
genericPeek :: BeanstalkServer -> String -> IO Job
genericPeek bs cmd = withMVar bs task
    where task s =
              do send s (cmd++"\r\n")
                 response <- readLine s
                 checkForBeanstalkErrors response
                 let (jobid, bytes) = parseFoundIdLen response
                 (content,bytesRead) <- recvLen s (bytes)
                 recv s 2 -- Ending CRLF
                 return (Job jobid content)

-- | Move jobs from current tube into ready queue.  If buried jobs
--   exist, only those will be moved, otherwise delayed jobs will be
--   made ready.
kickJobs :: BeanstalkServer -- ^ Beanstalk server
         -> Int -- ^ Number of jobs to kick
         -> IO Int -- ^ Number of jobs actually kicked
kickJobs bs maxcount = withMVar bs task
    where task s =
              do send s ("kick "++(show maxcount)++"\r\n")
                 response <- readLine s
                 checkForBeanstalkErrors response
                 return (parseKicked response)

parseKicked :: String -> Int
parseKicked input = case kparse of
                      Right a -> read a
                      Left _ -> 0 -- Error
    where kparse = parse (string "KICKED " >> many1 digit) "KickedParser" input

-- Essence of the various stats commands.  Variations provide the
-- command, while this function actually executes it and parses the
-- results.
genericStats :: BeanstalkServer -> String -> IO (M.Map String String)
genericStats bs cmd = withMVar bs task
    where task s =
              do send s (cmd++"\r\n")
                 statHeader <- readLine s
                 checkForBeanstalkErrors statHeader
                 let bytes = parseOkLen statHeader
                 (statContent, bytesRead) <- recvLen s (bytes)
                 recv s 2 -- Ending CRLF
                 yamlN <- parseYaml statContent
                 return $ yamlMapToHMap yamlN

-- | Return statistical information about a job.  Keys that can be
--   expected to be returned are the following:
--
--   [@id@] ID of the job.
--
--   [@tube@] The tube that contains this job
--
--   [@state@] State of the job, either \"ready\", \"delayed\", \"reserved\", or \"buried\"
--
--   [@pri@] Priority of the job
--
--   [@age@] Time in seconds since the 'putJob' command created this job
--
--   [@time-left@] Time in seconds until this job is placed in the
--   ready queue, if it is currently reserved or delayed
--
--   [@reserves@] Number of times this job has been reserved
--
--   [@timeouts@] Number of times this job has timed out after a reservation
--
--   [@releases@] Number of times this job has been released
--
--   [@buries@] Number of times this job has been buried
--
--   [@kicks@] Number of times this job has been kicked
--
--   See the Beanstalk protocol docs for the definitive list and definitions.
statsJob :: BeanstalkServer -- ^ Beanstalk server
         -> Int -- ^ ID of job
         -> IO (M.Map String String) -- ^ Key-value map of job statistics
statsJob bs jobid = genericStats bs ("stats-job "++(show jobid))

-- | Return statistical information about a tube.  Keys that can be
--   expected to be returned are the following:
--   [@name@] Name of the tube
--
--   [@current-jobs-urgent@] Number of jobs in this tube with state
--   'READY', with priority less than 1024
--
--   [@current-jobs-ready@] Number of jobs in this tube with state
--   'READY'
--
--   [@current-jobs-reserved@] Number of jobs in this tube with state
--   'RESERVED'
--
--   [@current-jobs-delayed@] Number of jobs in this tube with state
--   'DELAYED'
--
--   [@current-jobs-buried@] Number of jobs in this tube with state
--   'BURIED'
--
--   [@total-jobs@] Number of jobs that have been created in this tube
--   since it was created
--
--   [@current-waiting@] Number of clients that have issued a reserve
--   command for this tube, and are still blocking waiting on a
--   response
--
--   [@pause@] Number of seconds this tube has been paused
--
--   [@cmd-pause-tube@] Number of seconds this tube has been paused
--
--   [@pause-time-left@] Seconds remaining until this tube accepts job
--   reservations
--
--   See the Beanstalk protocol docs for the definitive list and definitions.
statsTube :: BeanstalkServer -- ^ Beanstalk server
          -> String -- ^ Name of tube
          -> IO (M.Map String String) -- ^ Key-value map of tube statistics
statsTube bs tube = genericStats bs ("stats-tube "++tube)

-- | Print stats to screen in a readable format.
printStats :: M.Map String String -- ^ Key-value map of statistic names and values
           -> IO () -- ^ Screen output showing all \"key => value\" pairs
printStats stats =
    do let kv = M.assocs stats
       mapM_ (\(k,v) -> putStrLn (k ++ " => " ++ v)) kv

-- | Pretty print a list.
printList :: [String] -- ^ List of names
          -> IO () -- ^ Screen output showing results with prefixed counter
printList list =
    do mapM_ (\(n,x) -> putStrLn (" "++(show n)++". "++x)) (zip [1..] (list))

-- | Return statistical information about the server, across all
--   clients.  Keys that can be expected to be returned are the
--   following:
--
--   [@current-jobs-urgent@] Number of 'READY' jobs with priority less
--   than 1024
--
--   [@current-jobs-ready@] Number of jobs in the ready queue
--
--   [@current-jobs-reserved@] Number of jobs reserved
--
--   [@current-jobs-delayed@] Number of delayed jobs
--
--   [@current-jobs-buried@] Number of buried jobs
--
--   [@cmd-put@] Cumulative number of 'putJob' commands issued
--
--   [@cmd-peek@] Cumulative number of 'peekJob' commands issued
--
--   [@cmd-peek-ready@] Cumulative number of 'peekReadyJob' commands
--   issued
--
--   [@cmd-peek-delayed@] Cumulative number of 'peekDelayedJob'
--   commands issued
--
--   [@cmd-peek-buried@] Cumulative number of 'peekBuriedJob' commands
--   issued
--
--   [@cmd-reserve@] Cumulative number of 'reserveJob' commands issued
--
--   [@cmd-use@] Cumulative number of 'useTube' commands issued
--
--   [@cmd-watch@] Cumulative number of 'watchTube' commands issued
--
--   [@cmd-ignore@] Cumulative number of 'ignoreTube' commands issued
--
--   [@cmd-delete@] Cumulative number of 'deleteJob' commands issued
--
--   [@cmd-release@] Cumulative number of 'releaseJob' commands issued
--
--   [@cmd-bury@] Cumulative number of 'buryJob' commands issued
--
--   [@cmd-kick@] Cumulative number of 'kickJobs' commands issued
--
--   [@cmd-stats@] Cumulative number of 'statsServer' commands issued
--
--   [@cmd-stats-job@] Cumulative number of 'statsJob' commands issued
--
--   [@cmd-stats-tube@] Cumulative number of 'statsTube' commands
--   issued
--
--   [@cmd-list-tubes@] Cumulative number of 'listTubes' commands
--   issued
--
--   [@cmd-list-tube-used@] Cumulative number of 'listTubeUsed'
--   commands issued
--
--   [@cmd-list-tubes-watched@] Cumulative number of
--   'listTubesWatched' commands issued
--
--   [@cmd-pause-tube@] Cumulative number of 'pauseTube' commands
--   issued
--
--   [@job-timeouts@] Cumulative number of times a job has timed out
--
--   [@total-jobs@] Total count of jobs created
--
--   [@max-job-size@] Maximum number of bytes for a job body
--
--   [@current-tubes@] Current number of existing tubes
--
--   [@current-connections@] Number of currently open connections
--
--   [@current-producers@] Number of currently open connections that
--   have issued at least one 'putJob' command
--
--   [@current-workers@] Number of currently open connections that
--   have issued at least one 'reserveJob' command
--
--   [@current-waiting@] Number of currently open connections that are
--   blocking on a 'reserveJob' or 'reserveJobWithTimeout' command
--
--   [@total-connections@] Cumulative count of connections
--
--   [@pid@] Process ID of the server
--
--   [@version@] Server's version string
--
--   [@rusage-utime@] The accumulated user CPU time of the server
--   process in seconds and microseconds
--
--   [@rusage-stime@] The accumulated system CPU time of the server
--   process in seconds and microseconds
--
--   [@uptime@] The number of seconds since the server started
--
--   [@binlog-oldest-index@] The index of the oldest binlog file
--   needed to store the current jobs
--
--   [@binlog-current-index@] The index of the current binlog file
--   being written to.  If the binlog is not active, this is zero
--
--   [@binlog-max-size@] The maximum number of bytes for a binlog file
--   before a new binlog file is opened
--
--   See the Beanstalk protocol docs for the definitive list and definitions.
statsServer :: BeanstalkServer -- ^ Beanstalk server
            -> IO (M.Map String String) -- ^ Key-value map of server statistics
statsServer bs = genericStats bs "stats"

-- | List all existing tubes.
listTubes :: BeanstalkServer -- ^ Beanstalk server
          -> IO [String] -- ^ Names of all tubes on the server
listTubes bs = genericList bs "list-tubes"

-- | List all watched tubes.
listTubesWatched :: BeanstalkServer -- ^ Beanstalk server
                 -> IO [String] -- ^ Names of all currently watched tubes
listTubesWatched bs = genericList bs "list-tubes-watched"

-- | List used tube.
listTubeUsed :: BeanstalkServer -- ^ Beanstalk server
             -> IO String -- ^ Name of current used tube
listTubeUsed bs = withMVar bs task
    where task s =
              do send s ("list-tube-used\r\n")
                 response <- readLine s
                 checkForBeanstalkErrors response
                 let tubeName = parseUsedTube response
                 return tubeName

parseUsedTube :: String -> String
parseUsedTube input =
    case (parse usedTubeParser "UsedTubeParser" input) of
      Right x -> x
      Left _ -> ""

nameParser = do initial <- leadingNameParser
                rest <- many1 (leadingNameParser <|> char '-')
                return (initial : rest)
                    where leadingNameParser = alphaNum <|> oneOf "+/;.$_()"

usedTubeParser = do string "USING "
                    tube <- nameParser
                    string "\r\n"
                    return tube

-- Essence of list commands that return YAML lists.
genericList :: BeanstalkServer -> String -> IO [String]
genericList bs cmd = withMVar bs task
    where task s =
              do send s (cmd++"\r\n")
                 lHeader <- readLine s
                 checkForBeanstalkErrors lHeader
                 let bytes = parseOkLen lHeader
                 (content, bytesRead) <- recvLen s (bytes)
                 recv s 2 -- Ending CRLF
                 yamlN <- parseYaml content
                 return $ yamlListToHList yamlN

-- | Count number of jobs in a tube with a state in a given list.
--   This is not part of the beanstalk protocol spec, so multiple
--   commands are issued to retrieve the count.  Therefore, the result
--   may not be consistent (it does not represent one snapshot in
--   time).
jobCountWithState :: BeanstalkServer -- ^ Beanstalk server
                   -> String -- ^ Name of tube to inspect
                   -> [JobState] -- ^ List of valid states for count
                   -> IO Int -- ^ Number of jobs with a state in the valid list
jobCountWithState bs tube validStatuses =
    do ts <- statsTube bs tube
       let readyCount = case (elem READY validStatuses) of
                          True -> read (fromJust (M.lookup "current-jobs-ready" ts))
                          False -> 0
       let reservedCount = case (elem RESERVED validStatuses) of
                          True -> read (fromJust (M.lookup "current-jobs-reserved" ts))
                          False -> 0
       let delayedCount = case (elem DELAYED validStatuses) of
                          True -> read (fromJust (M.lookup "current-jobs-delayed" ts))
                          False -> 0
       let buriedCount = case (elem BURIED validStatuses) of
                          True -> read (fromJust (M.lookup "current-jobs-buried" ts))
                          False -> 0
       return (readyCount+reservedCount+delayedCount+buriedCount)

yamlListToHList :: YamlNode -> [String]
yamlListToHList y = elems where
    elist = (n_elem y)
    ESeq list = elist
    yelems = map n_elem list
    elems = map (\(EStr x) -> unpackBuf x) yelems

yamlMapToHMap :: YamlNode -> M.Map String String
yamlMapToHMap y = M.fromList elems where
    emap = (n_elem y)
    EMap maplist = emap -- [(YamlNode,YamlNode)]
    yelems = map (\(x,y) -> (n_elem x, n_elem y))  maplist
    elems = map (\(EStr x, EStr y) -> (unpackBuf x, unpackBuf y)) yelems

-- Read a single character from socket without handling errors.
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

-- Parse response from watch/ignore command to determine how many
-- tubes are currently being watched.
parseWatching :: String -> Int
parseWatching input =
    case (parse (string "WATCHING " >> many1 digit) "WatchParser" input) of
      Right x -> read x
      Left _ -> 0

-- Parse response from put command.
parsePut :: String -> (JobState, Int)
parsePut input =
    case (parse putParser "PutParser" input) of
      Right x -> x
      Left _ -> (READY, 0) -- Error

putParser = do stateStr <- many1 letter
               char ' '
               jobid <- many1 digit
               let state = case stateStr of
                             "BURIED" -> BURIED
                             _ -> READY
               return (state, read jobid)

-- Get Job ID and size.
parseReserve :: String -> (String,Int)
parseReserve input =
    case (parse reservedParser "ReservedParser" input) of
      Right (x,y) -> (x, read y)
      Left _ -> ("",0)

-- Parse response from reserve command, including job id and bytes of body
-- to come next.
reservedParser :: GenParser Char st (String,String)
reservedParser = do string "RESERVED"
                    char ' '
                    x <- many1 digit
                    char ' '
                    y <- many1 digit
                    return (x,y)

-- Get number of bytes from an OK <bytes> response string.
parseOkLen :: String -> Int
parseOkLen input =
        case (parse okLenParser "okLenParser" input) of
          Right len -> read len
          Left err -> 0

-- Parser for first line of stats for data length indicator.
okLenParser :: GenParser Char st String
okLenParser = string "OK " >> many1 digit

-- Get job id and number of bytes from FOUND response string.
parseFoundIdLen :: String -> (Int,Int)
parseFoundIdLen input =
    case (parse foundIdLenParser "FoundIdLenParser" input) of
      Right x -> x
      Left _ -> (0,0)

foundIdLenParser :: GenParser Char st (Int,Int)
foundIdLenParser = do string "FOUND "
                      jobid <- many1 digit
                      string " "
                      bytes <- many1 digit
                      return (read jobid, read bytes)
