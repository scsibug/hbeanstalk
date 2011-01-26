#!/usr/bin/env runhaskell
-----------------------------------------------------------------------------
-- |
-- Program     :  hbeanstalk producer
-- Copyright   :  (c) Greg Heartsfield 2010
-- License     :  BSD3
--
-- Produce random jobs repeatedly, and put them on the default tube.
-- Usage:
--    hb_producer.hs host port delay
-----------------------------------------------------------------------------

import Network.Beanstalk
import System.Environment
import Control.Concurrent
import System.Random (randomIO)
import qualified Data.ByteString.Char8 as B

main = do argv <- getArgs
          let host : port : delay : xs = argv
          bs <- connectBeanstalk host port
          produceJobs bs (read delay)

produceJobs bs d =
    do rand_body <- randomBody
       (_,id) <- putJob bs 1 0 10 rand_body
       threadDelay (1000*1000*d) -- sleep d seconds
       putStrLn ("created job #"++(show id))
       produceJobs bs d

randomBody =
    do rdata <- randomIO :: IO Integer
       return (B.pack $ "job data="++(show (abs rdata)))
