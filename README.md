# Bonobo

Bonobo is a query engine that uses the [Arrow](https://github.com/apache/arrow) type system and aims for maximum compatibility with the [Substrait](https://github.com/substrait-io/substrait) specification. It is capable of:
- Creating plans via SQL or a Dataframe API
- Serializing plans to and from Substrait
- Manipulating/Splitting/Validating query plans

Bonobo does not, by design, implement execution kernels for any plan relations or expressions. Instead, Bonobo aims to make it simple to delegate plans to any Substrait-compatible backends for distributed execution.

## Install

```shell
go get github.com/joellubi/bonobo
```

## Roadmap

Bonobo is still in early stages of development. Features are being added quickly and the API is subject to change.

For a feature to be considered implemented it should:
- Be supported using the Dataframe API
- Implement both serialization and deserialization to/from Substrait proto

Features:
- [ ] Logical Relations
  - [ ] ReadRel
    - [x] NamedTable
    - [x] VirtualTable
    - [ ] ExtensionTable
    - [ ] LocalFiles
    - [ ] Filter
    - [ ] Project
  - [x] FilterRel
  - [ ] FetchRel
  - [ ] AggregateRel
  - [ ] SortRel
  - [ ] JoinRel
  - [x] ProjectRel
  - [ ] SetRel
  - [ ] ExtensionSingleRel
  - [ ] ExtensionMultiRel
  - [ ] ExtensionLeafRel
  - [ ] CrossRel
  - [ ] ReferenceRel
  - [ ] WriteRel
  - [ ] DdlRel
- [ ] Physical Relations (TBD whether they will be supported)
- [ ] Expressions
  - [x] Literal
  - [x] FieldReference
  - [ ] ScalarFunction
  - [ ] WindowFunction
  - [ ] IfThen
  - [ ] SwitchExpression
  - [ ] SingularOrList
  - [ ] MultiOrList
  - [ ] Cast
  - [ ] Subquery
  - [ ] Nested
- [ ] Extensions
  - [ ] Simple Extensions
    - [ ] Type
    - [ ] Type Variation
    - [ ] Function
      - [ ] Scalar Function
      - [ ] Aggregate Function
      - [ ] Window Function
    - [ ] Type Syntax Parsing
  - [ ] Advanced Extensions
  - [ ] Capabilities
- [ ] SQL Support
  - [ ] Query Clauses
    - [x] SELECT
    - [x] FROM
    - [x] WHERE
    - [ ] GROUP BY
    - [ ] ORDER BY
    - [ ] PARTITION BY
    - [ ] LIMIT
    - [ ] JOIN
    - [ ] OVER
    - [ ] UNION
  - [x] Binary Operators
  - [x] Parenthesis
  - [x] Expression Aliases
  - [x] Identifier Aliases
  - [x] Table Subqueries
  - [ ] Scalar Subqueries
  - [ ] Functions
