#!/bin/bash

function clean_docs(){
  rm -rf docs/build
}

function clean_coverage(){
  rm -rf coverage
  rm -f .coverage coverage.xml
}

function clean_packaging(){
  rm -rf build dist bigquery_operator.egg-info
}

function clean(){
  clean_docs
  clean_coverage
  clean_packaging
}
