/*
KERNEL:

graphene kernel run --config ./graphene-kernel.yaml
--config ./graphene-kernel.yaml: path to the kernel configuration file

CI:

graphene ci init --lang go --path ./.graphene-ci
--lang go: CI implementation language
--path ./.graphene-ci: directory for the new CI project

graphene ci build (--config ./.graphene-ci/.graphene-ci.yaml)
--config ./.graphene-ci/.graphene-ci.yaml: path to the CI configuration file

graphene ci plan (--config ./.graphene-ci/.graphene-ci.yaml)
--config ./.graphene-ci/.graphene-ci.yaml: path to the CI configuration file

graphene ci run (--config ./.graphene-ci/.graphene-ci.yaml) (--watch)
--config ./.graphene-ci/.graphene-ci.yaml: path to the CI configuration file
--watch: watch the CI run until completion

BLOCK:

graphene block init --lang go --path ./someBlock
--lang go: block implementation language
--path ./someBlock: directory for the new block

graphene block gen --config ./.graphene-block.yaml
--config ./.graphene-block.yaml: path to the block configuration file

graphene block build --config ./.graphene-block.yaml
--config ./.graphene-block.yaml: path to the block configuration file
*/
package main
