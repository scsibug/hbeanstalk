#!/bin/sh
# Run tests, generate code coverage, and produce Haddock docs
rm Tests.tix; ghc -fhpc Tests.hs --make ; ./Tests ; hpc markup Tests
haddock --html Network.Beanstalk -o doc
