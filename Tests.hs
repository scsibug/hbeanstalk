-----------------------------------------------------------------------------
-- |
-- Module      :  Beanstalk Tests
-- Copyright   :  (c) Greg Heartsfield 2010
-- License     :  BSD3
--
-- Test hbeanstalkh library against a real beanstalkd server.
-- This script assumes the server is running on localhost:11300, and can
-- be executed with `runhaskell Tests.hs`
-----------------------------------------------------------------------------

module Main(main) where

import Network.Beanstalk
import Control.Exception(finally)
import IO(bracket)
import Control.Concurrent.MVar
import Network.Socket
import Test.HUnit

bs_host = "localhost"
bs_port = "11300"

-- | Run the tests
main = runTestTT tests

tests =
    TestList
    [
     TestLabel "Beanstalk Connect" beanstalkConnectTest
    ]


-- | Ensure that connection to a server works, or at least that no
--   exceptions are thrown.
beanstalkConnectTest =
    TestCase (
              do bs <- connectBeanstalk "localhost" "11300"
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