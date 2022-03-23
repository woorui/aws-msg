# aws-msg

![build](https://github.com/woorui/aws-msg/actions/workflows/go.yml/badge.svg)
[![codecov](https://codecov.io/gh/woorui/aws-msg/branch/main/graph/badge.svg?token=Y0030WHH14)](https://codecov.io/gh/woorui/aws-msg)

## Overview

Package aws-msg implements pub/sub primitives outlined in package "github.com/zerofox-oss/go-msg".

It's built on top of "github.com/aws/aws-sdk-go-v2".

## Why use

1. Elegant pub/sub API from https://github.com/zerofox-oss/go-msg

2. Support multiple goroutinue to pull messages from SQS; The receive speed is significantly improved.

3. Support graceful shutdown.

4. Backpressure for handling SQS message.
## Examples

https://github.com/woorui/aws-msg/blob/main/sqs/examples/example_test.go

## Reference

https://github.com/zerofox-oss/go-msg

https://github.com/aws/aws-sdk-go-v2

https://github.com/zerofox-oss/go-aws-msg
