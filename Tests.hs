-----------------------------------------------------------------------------
-- |
-- Module      :  Beanstalk Tests
-- Copyright   :  (c) Greg Heartsfield 2010
-- License     :  BSD3
--
-- Test hbeanstalkh library against a real beanstalkd server.
-- This script assumes the server is running on localhost:11300, and can
-- be executed with `runhaskell Tests.hs`
-- For best results, this should probably be run against a newly started
-- server with zero jobs (restart server, and run without persistence)
-----------------------------------------------------------------------------

module Main(main) where

import Network.Beanstalk
import Control.Exception(finally)
import IO(bracket)
import Control.Concurrent.MVar
import Network.Socket
import Test.HUnit
import System.Random (randomIO)
import Data.Maybe(fromJust)
import qualified Data.Map as M
import qualified Control.Exception as E
import Control.Monad

bs_host = "localhost"
bs_port = "11300"

-- | Run the tests
main = runTestTT tests

tests =
    TestList
    [
     TestLabel "Connect" connectTest,
     TestLabel "Use" useTest,
     TestLabel "Watch" watchTest,
     TestLabel "Put" putTest,
     TestLabel "Put2" putTest2,
     TestLabel "Put/Reserve" putReserveTest,
     TestLabel "Put/Reserve-With-Timeout" putReserveWithTimeoutTest,
     TestLabel "Peek" peekTest,
     TestLabel "KickDelay" kickDelayTest,
     TestLabel "Release" releaseTest,
     TestLabel "Ignore" ignoreTest,
     TestLabel "Delete" deleteTest,
     TestLabel "Bury" buryTest,
     TestLabel "PeekReady" peekReadyTest,
     TestLabel "PeekJob" peekJobTest,
     TestLabel "PeekDelayed" peekDelayedTest,
     TestLabel "PeekBuried" peekBuriedTest,
     TestLabel "StatsJob" statsJobTest,
     TestLabel "ServerStats" statsTest,
     TestLabel "ListTubes" listTubesTest,
     TestLabel "ListTubesWatched" listTubesWatchedTest,
     TestLabel "ListTubeUsed" listTubeUsedTest,
     TestLabel "isNotfoundException" isNotFoundExceptionTest,
     TestLabel "isBadFormatException" isBadFormatxceptionTest,
     TestLabel "isTimedOutException" isTimedOutExceptionTest,
     TestLabel "AllExceptions" allExceptionTest,
     TestLabel "Disconnect" disconnectTest
    ]

-- | Ensure that connection to a server works, or at least that no
--   exceptions are thrown.
connectTest =
    TestCase (
              do bs <- connectBeanstalk bs_host bs_port
                 mbSock <- tryTakeMVar bs
                 case mbSock of
                   Nothing -> do assertFailure "Beanstalk socket was not in the MVar as expected."
                   Just s ->
                             do sIsConnected s @? "Beanstalk socket was not connected"
                                (sIsBound s >>= return.not) @? "Beanstalk socket was bound"
                                (sIsListening s >>= return.not) @? "Beanstalk socket was not listening"
                                sIsReadable s @? "Beanstalk socket was not readable"
                                sIsWritable s @? "Beanstalk socket was not writable"
             )

-- Test that using a tube doesn't cause exceptions.
useTest =
    TestCase (
              do bs <- connectBeanstalk bs_host bs_port
                 randomName >>= useTube bs
                 return ()
             )

-- Test that watching a tube works.
watchTest =
    TestCase (
              do bs <- connectBeanstalk bs_host bs_port
                 tubeName <- randomName
                 watchCount <- watchTube bs tubeName
                 assertEqual "Watch list should consist of 'default' and newly watched tube"
                                 2 watchCount
             )

-- Test that ignoring a tube works
ignoreTest =
    TestCase (
              do bs <- connectBeanstalk bs_host bs_port
                 tubeName <- randomName
                 watchCount <- watchTube bs tubeName
                 assertEqual "Watch list should consist of 'default' and newly watched tube"
                                 2 watchCount
                 newWatchCount <- ignoreTube bs "default"
                 assertEqual "Watch list should consist of newly watched tube only"
                                 1 newWatchCount
             )

-- Simply test that connecting and putting a job in the default tube works without exceptions.
putTest =
    TestCase (
              do bs <- connectBeanstalk bs_host bs_port
                 (state, jobid) <- putJob bs 1 0 60 "body"
                 return ()
             )

-- More exhaustive test of Put in a new tube
putTest2 =
    TestCase (
              do (bs, tt) <- connectAndSelectRandomTube
                 assertJobsCount bs tt [READY] 0 "Initially, no jobs"
                 (state, jobid) <- putJob bs 1 0 60 "body"
                 -- Technically could be BURIED, but only if memory exhausted.
                 assertEqual "New job is in state READY" READY state
                 assertJobsCount bs tt [READY] 1 "Put creates a ready job in the tube"
                 return ()
             )
-- Test putting and then reserving a job
putReserveTest =
    TestCase (
              do (bs, tt) <- connectAndSelectRandomTube
                 randString <- randomName
                 let body = "My test job body, " ++ randString
                 (_,put_job_id) <- putJob bs 1 0 60 body
                 rsv_job <- reserveJob bs
                 assertEqual "Reserved job ID should match what was put" put_job_id (job_id rsv_job)
                 assertEqual "Reserved job body should match what was put" body (job_body rsv_job)
                 assertEqual "Reserved job should match job that was just put"
                             put_job_id (job_id rsv_job)
             )

-- Test putting and then reserving a job with timeout
putReserveWithTimeoutTest =
    TestCase (
              do (bs, tt) <- connectAndSelectRandomTube
                 randString <- randomName
                 let body = "My test job body, " ++ randString
                 (_,put_job_id) <- putJob bs 1 0 60 body
                 rsv_job <- reserveJobWithTimeout bs 2
                 assertEqual "Reserved job should match job that was just put"
                             put_job_id (job_id rsv_job)
             )

-- Test peeking for a couple specific jobs
peekTest =
    TestCase (
              do (bs, tt) <- connectAndSelectRandomTube
                 randString <- randomName
                 let body = "My test job body, " ++ randString
                 (_,put_job_id) <- putJob bs 1 0 60 body
                 let next_body = "My test job body, " ++ randString
                 (_,put_next_job_id) <- putJob bs 1 0 60 next_body
                 peeked_job <- peekJob bs put_job_id
                 assertEqual "Peeked job id should match job id that was just put"
                             put_job_id (job_id peeked_job)
                 assertEqual "Peeked job should match job that was just put"
                             body (job_body peeked_job)
                 next_peeked_job <- peekJob bs put_next_job_id
                 assertEqual "Peeked job id should match job id that was just put"
                             put_next_job_id (job_id next_peeked_job)
                 assertEqual "Peeked job should match job that was just put"
                             next_body (job_body next_peeked_job)
             )
-- Test kicking a delayed job.
kickDelayTest =
    TestCase (
              do (bs, tt) <- connectAndSelectRandomTube
                 randString <- randomName
                 let body = "My test job body, " ++ randString
                 (_,put_job_id) <- putJob bs 1 5 60 body
                 kicked <- kickJobs bs 1
                 assertEqual "Kick should indicate one job kicked" 1 kicked
             )

-- Test putting a job, reserving it, and then releasing it back to the tube.
releaseTest =
    TestCase (
              do (bs, tt) <- connectAndSelectRandomTube
                 assertJobsCount bs tt [READY] 0 "New tube has no jobs"
                 -- Put a job on the tube
                 randString <- randomName
                 let body = "My test job body, " ++ randString
                 (_,put_job_id) <- putJob bs 1 0 60 body
                 assertJobsCount bs tt [READY] 1 "Put adds job to tube"
                 -- Reserve the job
                 rj <- reserveJob bs
                 assertJobsCount bs tt [READY] 0 "Reserve removes ready job"
                 assertJobsCount bs tt [RESERVED] 1 "Single reserved job"
                 -- Release it
                 releaseJob bs (job_id rj) 1 0
                 assertJobsCount bs tt [READY] 1 "Release puts job back to ready"
              )

-- Test deleting a reserved job.
deleteTest =
    TestCase (
              do (bs, tt) <- connectAndSelectRandomTube
                 assertJobsCount bs tt [READY] 0 "New tube has no jobs"
                 (_,put_job_id) <- putJob bs 1 0 60 "new job"
                 assertJobsCount bs tt [READY] 1 "Put creates new ready job"
                 job <- reserveJob bs
                 assertJobsCount bs tt [RESERVED] 1 "Only job on tube is reserved"
                 deleteJob bs (job_id job)
                 assertJobsCount bs tt [READY,RESERVED,DELAYED,BURIED] 0 "Tube is empty"
             )

-- Test burying a reserved job.
buryTest =
    TestCase (
              do (bs, tt) <- connectAndSelectRandomTube
                 assertJobsCount bs tt [READY] 0 "New tube has no jobs"
                 (_,put_job_id) <- putJob bs 1 0 60 "new job"
                 assertJobsCount bs tt [READY] 1 "Put creates new ready job"
                 job <- reserveJob bs
                 assertJobsCount bs tt [RESERVED] 1 "Only job on tube is reserved"
                 buryJob bs (job_id job) 1
                 assertJobsCount bs tt [BURIED] 1 "Job is buried"
             )

-- Test peeking to find the next ready job.
peekReadyTest =
    TestCase (
              do (bs, tt) <- connectAndSelectRandomTube
                 assertJobsCount bs tt [READY] 0 "New tube has no jobs"
                 (_,put_job_id) <- putJob bs 1 0 60 "new job"
                 assertJobsCount bs tt [READY] 1 "Put creates new ready job"
                 job <- peekReadyJob bs
                 assertJobsCount bs tt [READY] 1 "Job is still ready"
                 assertEqual "Peeked job id is same as put job" put_job_id (job_id job)
             )

-- Test peeking to find definition of a specific queued job.
peekJobTest =
    TestCase (
              do (bs, tt) <- connectAndSelectRandomTube
                 assertJobsCount bs tt [READY] 0 "New tube has no jobs"
                 randString <- randomName
                 let jobcontent = "new job "++randString
                 (_,put_job_id) <- putJob bs 1 0 60 jobcontent
                 assertJobsCount bs tt [READY] 1 "Put creates new ready job"
                 job <- peekJob bs put_job_id
                 assertJobsCount bs tt [READY] 1 "Job is still ready"
                 assertEqual "Peeked job id is same as put job" put_job_id (job_id job)
                 assertEqual "Peeked job content is same as put job" jobcontent (job_body job)
             )

-- Test peeking to find the next delayed job.
peekDelayedTest =
    TestCase (
              do (bs, tt) <- connectAndSelectRandomTube
                 assertJobsCount bs tt [READY] 0 "New tube has no jobs"
                 (_,put_job_id) <- putJob bs 1 120 60 "new job"
                 assertJobsCount bs tt [DELAYED] 1 "Put with delay"
                 job <- peekDelayedJob bs
                 assertJobsCount bs tt [DELAYED] 1 "Job is still delayed"
                 assertEqual "Peeked job id is same as put job" put_job_id (job_id job)
             )

-- Test peeking to find the next buried job.
peekBuriedTest =
    TestCase (
              do (bs, tt) <- connectAndSelectRandomTube
                 assertJobsCount bs tt [READY] 0 "New tube has no jobs"
                 (_,put_job_id) <- putJob bs 1 0 60 "new job"
                 assertJobsCount bs tt [READY] 1 "Put creates new ready job"
                 rsv_job <- reserveJob bs
                 assertJobsCount bs tt [RESERVED] 1 "Reserved job"
                 buryJob bs put_job_id 1
                 assertJobsCount bs tt [BURIED] 1 "Burid job"
                 job <- peekBuriedJob bs
                 assertJobsCount bs tt [BURIED] 1 "Job is still buried"
                 assertEqual "Peeked job id is same as put job" put_job_id (job_id job)
             )

-- Test finding information on a specific job.
statsJobTest =
    TestCase (
              do (bs, tt) <- connectAndSelectRandomTube
                 let priority = 99
                 (job_state ,put_job_id) <- putJob bs priority 0 60 "new job"
                 job_stats <- statsJob bs put_job_id
                 assertEqual "Job ID matches" put_job_id (read (fromJust (M.lookup "id" job_stats)))
                 assertEqual "Job priority matches" priority (read (fromJust (M.lookup "pri" job_stats)))
             )

-- Test finding server statistics.
statsTest =
    TestCase (
              do (bs, tt) <- connectAndSelectRandomTube
                 stats <- statsServer bs
                 assertBool "More than 1 job has been created" (1 < (read (fromJust (M.lookup "total-jobs" stats))))
             )

-- Test listing all tubes for the server.
listTubesTest =
    TestCase (
              do (bs, tt) <- connectAndSelectRandomTube
                 tubes <- listTubes bs
                 assertBool "Newly created tube is in list" (elem tt tubes)
             )

-- Test listing all watched tubes.
listTubesWatchedTest =
    TestCase (
              do (bs, tt) <- connectAndSelectRandomTube
                 -- Watch another tube so that we avaid NotIgnoredExceptions
                 otherTube <- randomName
                 watchTube bs otherTube
                 tubes <- listTubesWatched bs
                 assertBool "Newly created/watched tube is in watch list" (elem tt tubes)
                 ignoreTube bs tt
                 assertBool "Ignored tube is not in watch list" (elem tt tubes)
             )

-- Test listing the currently used tube.
listTubeUsedTest =
    TestCase (
              do (bs, tt) <- connectAndSelectRandomTube
                 tu <- listTubeUsed bs
                 assertEqual "Used tube" tt tu
             )

-- Test that NotFoundException is thrown
isNotFoundExceptionTest =
    TestCase (
              do (bs, tt) <- connectAndSelectRandomTube
                 e <- E.tryJust (guard . isNotFoundException) (deleteJob bs 999999)
                 case e of
                   Right _ -> assertFailure "Deleting non-existent job should fail"
                   Left _ -> return ()
             )

-- Test that BadFormatException is thrown
isBadFormatxceptionTest =
    TestCase (
              do (bs, tt) <- connectAndSelectRandomTube
                 rname <- randomName
                 e <- E.tryJust (guard . isBadFormatException) (statsTube bs ("-"++rname))
                 case e of
                   Right _ -> assertFailure "Using tube name starting with hyphen should fail"
                   Left _ -> return ()
             )

-- Test that TimedOutException is thrown
isTimedOutExceptionTest =
    TestCase (
              do (bs, tt) <- connectAndSelectRandomTube
                 e <- E.tryJust (guard . isTimedOutException) (reserveJobWithTimeout bs 1)
                 case e of
                   Right _ -> assertFailure "Reserve with no jobs causes timeout"
                   Left _ -> return ()
             )

-- Test all exception predicates to make sure they return false when
-- there are no errors.
allExceptionTest =
 TestCase (
           do (bs, tt) <- connectAndSelectRandomTube
              e <- E.tryJust (guard . isNotFoundException) (putJob bs 1 0 600 "test")
              case e of
                Right _ -> return ()
                Left _ -> assertFailure "Erroneous not found exception"
              e <- E.tryJust (guard . isBadFormatException) (putJob bs 1 0 600 "test")
              case e of
                Right _ -> return ()
                Left _ -> assertFailure "Erroneous bad format exception"
              e <- E.tryJust (guard . isTimedOutException) (putJob bs 1 0 600 "test")
              case e of
                Right _ -> return ()
                Left _ -> assertFailure "Erroneous timed out exception"
              e <- E.tryJust (guard . isOutOfMemoryException) (putJob bs 1 0 600 "test")
              case e of
                Right _ -> return ()
                Left _ -> assertFailure "(possible) Erroneous out of memory exception"
              e <- E.tryJust (guard . isInternalErrorException) (putJob bs 1 0 600 "test")
              case e of
                Right _ -> return ()
                Left _ -> assertFailure "(possible) Erroneous internal error exception"
              e <- E.tryJust (guard . isJobTooBigException) (putJob bs 1 0 600 "test")
              case e of
                Right _ -> return ()
                Left _ -> assertFailure "Erroneous job too big exception"
              e <- E.tryJust (guard . isDeadlineSoonException) (putJob bs 1 0 600 "test")
              case e of
                Right _ -> return ()
                Left _ -> assertFailure "Erroneous deadline soon exception"
              e <- E.tryJust (guard . isNotIgnoredException) (putJob bs 1 0 600 "test")
              case e of
                Right _ -> return ()
                Left _ -> assertFailure "Erroneous not ignored exception"
          )

disconnectTest =
    TestCase (
              do (bs, tt) <- connectAndSelectRandomTube
                 disconnectBeanstalk bs
                 mbSock <- tryTakeMVar bs
                 case mbSock of
                   Nothing -> do assertFailure "Beanstalk socket was not in the MVar as expected."
                   Just s ->
                             do (sIsConnected s >>= return .not) @? "Beanstalk socket was connected"
         )

-- Assert a number of jobs on a given tube with one of the states
-- listed.
assertJobsCount :: BeanstalkServer -> String -> [JobState] -> Int -> String -> IO ()
assertJobsCount bs tube states jobs msg =
    do ts <- statsTube bs tube
       jobsReady <- jobCountWithState bs tube states
       assertEqual msg jobs jobsReady


-- Configure a new beanstalkd connection to use&watch a single tube
-- with a random name.
connectAndSelectRandomTube :: IO (BeanstalkServer, String)
connectAndSelectRandomTube =
    do bs <- connectBeanstalk bs_host bs_port
       tt <- randomName
       useTube bs tt
       watchTube bs tt
       ignoreTube bs "default"
       return (bs, tt)

-- Generate random tube names for test separation.
randomName :: IO String
randomName =
    do rdata <- randomIO :: IO Integer
       return (show (abs rdata))
