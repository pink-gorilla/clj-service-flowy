# clj-service-flowy [![GitHub Actions status |pink-gorilla/clj-service-flowy](https://github.com/pink-gorilla/clj-service-flowy/workflows/CI/badge.svg)](https://github.com/pink-gorilla/clj-service-flowy/actions?workflow=CI)[![Clojars Project](https://img.shields.io/clojars/v/org.pinkgorilla/clj-service-flowy.svg)](https://clojars.org/org.pinkgorilla/clj-service-flowy)


stage: totally experimental

Goal: Make it easier to build reactive web applications in clojure/clojurescript.

There is hyperfiddle electric, which is amazing, and you should use it,
but if you cannot, then clj-service-flowy could be a good start. We stole the websocket part
of electric as base.

clj-service-flowy allows to expose clojure functions to a browser via websocket,
and for browser-ui to use them.

Exposed functions can
- be blocking and then return a clojure value (or an error)
- return a missionary task that returs just one value (or an error)
- return a missionary flow that can return mutliple values
- all exposed functions can be cancelled.

Then in the browser you deal with "proxies" of the tasks/flows in clojure
on the server.

# Reagent integration

clj-service-flowy.reagent/flow->ratom can be used in a reagent.core/with-let. It
gets 2 parameters: 1. the flow and 2. the initial value, It retruns
[flow-state-a dispose!].  The dispose! function needs to be used in the 
finally section of the with-let. flow-state-a is a normal reagent atom.

## Reagent demo

```
  cd demo-standalone
  npm install
  npm run compile-reagent
  clj -M:app  
```  

Open Browser on localhost:9000

# Pitch/UIX integration

clj-service-flowy.uix/use-flow is a react hook that returns the current value of a flow,
its parameters are 1. the flow and 2. the initial value that should be returned
before the flow returns something.

## Pitch/UIX Demo

```
  cd demo-standalone
  npm install
  npm run compile
  clj -M:app  
```  

 Open Browser on localhost:9000
 

## Reagent using webly demo

```
  cd demo-webly
  1. install npm deps
  clj -X:build :profile "npm-install"
  
  2. compile cljs using one of the following:
  clj -X:build :profile "compile"
  clj -X:build :profile "release"
  clj -X:build :profile "release-adv"

  3. run the app using one of the following
  clj -X:run or
  clj -X:run :profile '"watch"'
  clj -X:run :profile '"jetty"'
  
  compiled mode is localhost:9090 and watch mode is localhost:8000

```  
Open Browser on localhost:9090 

# Babashka Client

This is an experiment to see if babashka can be used as a commandline inspection tool;
the fast startup time makes sense. I doubt that missionary will run in babashka, but
one can use the protocol that we designed without missionary.

bb client

you can run multiple clients.


# Bundlesize

```  
npm run compile
npm run report

```  

333K without transit      330k is clojurescript
402K with transit          70k is transit
409K with task executor   200k is react/uix
602K with uix2 and react
