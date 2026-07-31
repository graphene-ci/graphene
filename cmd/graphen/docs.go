/*
KERNEL:

graphen kernel run --config ./graphen-kernel.yaml
--config ./graphen-kernel.yaml: path to the kernel configuration file

CI:

graphen ci init --lang go --path ./.graphen-ci
--lang go: CI implementation language
--path ./.graphen-ci: directory for the new CI project

graphen ci build (--config ./.graphen-ci/.graphen-ci.yaml)
--config ./.graphen-ci/.graphen-ci.yaml: path to the CI configuration file

graphen ci plan (--config ./.graphen-ci/.graphen-ci.yaml)
--config ./.graphen-ci/.graphen-ci.yaml: path to the CI configuration file

graphen ci run (--config ./.graphen-ci/.graphen-ci.yaml) (--watch)
--config ./.graphen-ci/.graphen-ci.yaml: path to the CI configuration file
--watch: watch the CI run until completion

BLOCK:

graphen block init --lang go --path ./someBlock
--lang go: block implementation language
--path ./someBlock: directory for the new block

graphen block gen --config ./.graphen-block.yaml
--config ./.graphen-block.yaml: path to the block configuration file

graphen block build --config ./.graphen-block.yaml
--config ./.graphen-block.yaml: path to the block configuration file
*/
package main
