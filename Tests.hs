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

bs_host = "localhost"
bs_port = "11300"

-- | Run the tests
main = runTestTT tests

tests =
    TestList
    [
     TestLabel "Beanstalk Connect" beanstalkConnectTest,
     TestLabel "Use" useTest,
     TestLabel "Watch" watchTest,
     TestLabel "Put" putTest,
     TestLabel "Put/Reserve" putReserveTest,
     TestLabel "Put/Reserve-With-Timeout" putReserveWithTimeoutTest
    ]


-- | Ensure that connection to a server works, or at least that no
--   exceptions are thrown.
beanstalkConnectTest =
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
                 job <- putJob bs 1 0 60 "body"
                 return ()
             )

-- Test putting and then reserving a job
putReserveTest =
    TestCase (
              do (bs, tt) <- connectAndSelectRandomTube
                 randString <- randomName
                 let body = "My test job body, " ++ randString
                 put_job_id <- putJob bs 1 0 60 body
                 rsv_job <- reserveJob bs
                 assertEqual "Reserved job should match job that was just put"
                             put_job_id (job_id rsv_job)
             )

-- Test putting and then reserving a job with timeout
putReserveWithTimeoutTest =
    TestCase (
              do (bs, tt) <- connectAndSelectRandomTube
                 randString <- randomName
                 let body = "My test job body, " ++ randString
                 put_job_id <- putJob bs 1 0 60 body
                 rsv_job <- reserveJobWithTimeout bs 2
                 assertEqual "Reserved job should match job that was just put"
                             put_job_id (job_id rsv_job)
             )



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
