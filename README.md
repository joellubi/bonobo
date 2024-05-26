# Bonobo

Bonobo is a query engine that uses the [Arrow](https://github.com/apache/arrow) type system and aims for maximum compatibility with the [Substrait](https://github.com/substrait-io/substrait) specification. It is capable of:
- Creating plans via SQL or a Dataframe API
- Serializing plans to and from Substrait
- Manipulating/Splitting/Validating query plans

Bonobo does not, by design, implement execution kernels for any plan relations or expressions. Instead, Bonobo aims to make it simple to delegate plans to any Substrait-compatible backends for execution.

## Install

```shell
go get github.com/joellubi/bonobo
```

## Roadmap

TODO...