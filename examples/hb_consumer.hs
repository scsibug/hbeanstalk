#!/usr/bin/env runhaskell
-----------------------------------------------------------------------------
-- |
-- Program     :  hbeanstalk consumer
-- Copyright   :  (c) Greg Heartsfield 2010
-- License     :  BSD3
--
-- Consume jobs from the default tube as quickly as possible
-- Usage:
--    hb_producer.hs host port
-----------------------------------------------------------------------------

import Network.Beanstalk
import System.Environment

main = do argv <- getArgs
          let host : port : xs = argv
          bs <- connectBeanstalk host port
          consumeJobs bs

consumeJobs bs =
    do job <- reserveJob bs
       putStrLn ("reserved job #"++(show (job_id job)))
       putStrLn ("   Job body: "++(job_body job))
       deleteJob bs (job_id job)
       consumeJobs bs