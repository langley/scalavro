# Scalavro

A runtime reflection-based Avro library in Scala.

Scalavro takes a code-first, reflection based approach to schema generation and (de)serialization.  This yields a very low-overhead interface, and imposes some costs.  In general, Scalavro assumes you know what types you're reading and writing.  No built-in support is provided (as yet) for so-called schema resolution (taking the writer's schema into account when reading data).

## Goals

1. To provide an in-memory representation of avro schemas and protocols.
2. To synthesize avro schemas and protocols dynamically for a useful subset of Scala types.
4. To dynamically generate Scala bindings for reading and writing Avro-mapped Scala types to and from Avro binary.
5. Generally, to minimize fuss required to create an Avro-capable Scala application.

## Obtaining Scalavro

The `Scalavro` artifacts are available from Maven Central. The current release is `0.3.1`, built against Scala 2.10.2.

Using SBT:

```scala
libraryDependencies += "com.gensler" %% "scalavro-io" % "0.3.1"
```

## API Documentation

- Generated [Scaladoc for version 0.3.1](http://genslerappspod.github.io/scalavro/api/0.3.1/index.html#com.gensler.scalavro.package)

## Index of Examples

- [Arrays](#arrays)
- [Maps](#maps)
- [Enums](#enums)
- [Unions](#unions)
- [Fixed-Length Data](#fixed)
- [Records](#records)
- [Binary IO](#binary-io)

## Type Mapping Strategy

### Primitive Types

<table>
  <thead>
  	<tr>
  	  <th>Scala Type</th>
  	  <th>Avro Type</th>
  	</tr>
  </thead>
  <tbody>
    <tr>
      <td><code>
        Unit
      </code></td>
      <td><code>
        null
      </code></td>
    </tr>
    <tr>
      <td><code>
        Boolean
      </code></td>
      <td><code>
        boolean
      </code></td>
    </tr>
    <tr>
      <td><code>
        Byte
      </code></td>
      <td><code>
        int
      </code></td>
    </tr>
    <tr>
      <td><code>
        Char
      </code></td>
      <td><code>
        int
      </code></td>
    </tr>
    <tr>
      <td><code>
        Short
      </code></td>
      <td><code>
        int
      </code></td>
    </tr>
    <tr>
      <td><code>
        Int
      </code></td>
      <td><code>
        int
      </code></td>
    </tr>
    <tr>
      <td><code>
        Long
      </code></td>
      <td><code>
        long
      </code></td>
    </tr>
    <tr>
      <td><code>
        Float
      </code></td>
      <td><code>
        float
      </code></td>
    </tr>
    <tr>
      <td><code>
        Double
      </code></td>
      <td><code>
        double
      </code></td>
    </tr>
    <tr>
      <td><code>
        String
      </code></td>
      <td><code>
        string
      </code></td>
    </tr>
    <tr>
      <td><code>
        scala.collection.Seq[Byte]
      </code></td>
      <td><code>
        bytes
      </code></td>
    </tr>
  </tbody>
</table>

### Complex Types

<table>
  <thead>
  	<tr>
  	  <th>Scala Type</th>
  	  <th>Avro Type</th>
  	</tr>
  </thead>
  <tbody>
    <tr>
      <td><code>
        scala.collection.Seq[T]
      </code></td>
      <td><code>
        array
      </code></td>
    </tr>
    <tr>
      <td><code>
        scala.collection.Set[T]
      </code></td>
      <td><code>
        array
      </code></td>
    </tr>
    <tr>
      <td><code>
        scala.collection.Map[String, T]
      </code></td>
      <td><code>
        map
      </code></td>
    </tr>
    <tr>
      <td><code>
        scala.Enumeration#Value
      </code></td>
      <td><code>
        enum
      </code></td>
    </tr>
    <tr>
      <td>
        <code>enum</code> (Java)</td>
      <td><code>
        enum
      </code></td>
    </tr>
    <tr>
      <td><code>
        scala.util.Either[A, B]
      </code></td>
      <td><code>
        union
      </code></td>
    </tr>
    <tr>
      <td><code>
        scala.util.Option[T]
      </code></td>
      <td><code>
        union
      </code></td>
    </tr>
    <tr>
      <td><code>
        com.gensler.scalavro.util.Union[U]
      </code></td>
      <td><code>
        union
      </code></td>
    </tr>
    <tr>
      <td><code>
        com.gensler.scalavro.util.FixedData
      </code></td>
      <td><code>
        fixed
      </code></td>
    </tr>
    <tr>
      <td><em>
        Supertypes of non-recursive case classes without type parameters
      </em></td>
      <td><code>
        union
      </code></td>
    </tr>
    <tr>
      <td><em>
        Non-recursive case classes without type parameters
      </em></td>
      <td><code>
        record
      </code></td>
    </tr>
  </tbody>
</table>

## General Information
- Built against Scala 2.10.2 with SBT 0.12.4
- Depends upon [spray-json](https://github.com/spray/spray-json)
- The `io` sub-project depends upon the Apache Java implementation of Avro (Version 1.7.4)

## Current Capabilities
- Dynamic Avro schema generation from vanilla Scala types
- Avro protocol definitions and schema generation
- Convenient, dynamic binary IO
- Avro RPC protocol representation and schema generation
- Schema conversion to "Parsing Canonical Form" (useful for Avro RPC protocol applications)

## Current Limitations
- JSON IO is not yet implemented
- Schema resolution (taking the writer's schema into account when reading) is not yet implemented
- Recursive type dependencies are detected but not handled optimally -- potentially valid types are rejected at runtime.  For example, the current version cannot synthesize an Avro schema for a simple recursively defined linked list node.  Supporting this is a planned enhancement.

## Scalavro by Example: Schema Generation

<a name="arrays"></a>
### Arrays

#### scala.Seq

```scala
import com.gensler.scalavro.types.AvroType
AvroType[Seq[String]].schema
```

Which yields:

```json
{
  "type" : "array",
  "items" : "string"
}
```

#### scala.Set

```scala
import com.gensler.scalavro.types.AvroType
AvroType[Set[String]].schema
```

Which yields:

```json
{
  "type" : "array",
  "items" : "string"
}
```

<a name="maps"></a>
### Maps

```scala
import com.gensler.scalavro.types.AvroType
AvroType[Map[String, Double]].schema
```

Which yields:

```json
{
  "type" : "map",
  "values" : "double"
}
```

<a name="enums"></a>
### Enums

#### scala.Enumeration

```scala
package com.gensler.scalavro.tests
import com.gensler.scalavro.types.AvroType

object CardinalDirection extends Enumeration {
  type CardinalDirection = Value
  val N, NE, E, SE, S, SW, W, NW = Value
}

import CardinalDirection._
AvroType[CardinalDirection].schema
```

Which yields:

```json
{
  "name" : "CardinalDirection",
  "type" : "enum",
  "symbols" : ["N","NE","E","SE","S","SW","W","NW"],
  "namespace" : "com.gensler.scalavro.tests.CardinalDirection"
}
```

#### Java `enum`

Definition (Java):

```java
package com.gensler.scalavro.tests;
enum JCardinalDirection { N, NE, E, SE, S, SW, W, NW };
```

Use (Scala):

```scala
import com.gensler.scalavro.types.AvroType
import com.gensler.scalavro.tests.JCardinalDirection

AvroType[JCardinalDirection].schema
```

Which yields:

```json
{
  "name" : "JCardinalDirection",
  "type" : "enum",
  "symbols" : ["N","NE","E","SE","S","SW","W","NW"],
  "namespace" : "com.gensler.scalavro.tests"
}
```

<a name="unions"></a>
### Unions

#### scala.Either

```scala
package com.gensler.scalavro.tests
import com.gensler.scalavro.types.AvroType

AvroType[Either[Int, Boolean]].schema
```

Which yields:

```json
["int", "boolean"]
```

and

```scala
AvroType[Either[Seq[Double], Map[String, Seq[Int]]]].schema
```

Which yields:

```json
[{
  "type" : "array",
  "items" : "double"
},
{
  "type" : "map",
  "values" : {
    "type" : "array",
    "items" : "int"
  }
}]
```

#### scala.Option

```scala
package com.gensler.scalavro.tests
import com.gensler.scalavro.types.AvroType

AvroType[Option[String]].schema
```

Which yields:

```json
["null", "string"]
```

#### com.gensler.scalavro.util.Union.union

```scala
import com.gensler.scalavro.types.AvroType
import com.gensler.scalavro.util.Union._

AvroType[union [Int] #or [String] #or [Boolean]].schema
```

Which yields:

```json
["int", "string", "boolean"]
```

<a name="fixed"></a>
### Fixed-Length Data

```scala
package com.gensler.scalavro.tests

import com.gensler.scalavro.types.AvroType
import com.gensler.scalavro.util.FixedData
import scala.collection.immutable

@FixedData.Length(16)
case class MD5(override val bytes: immutable.Seq[Byte])
           extends FixedData(bytes)

AvroType[MD5].schema
```

Which yields:

```json
{
  "name": "MD5",
  "type": "fixed",
  "size": 16,
  "namespace": "com.gensler.scalavro.tests"
}
```

<a name="records"></a>
### Records

#### From case classes

```scala
package com.gensler.scalavro.tests
import com.gensler.scalavro.types.AvroType

case class Person(name: String, age: Int)

val personAvroType = AvroType[Person]
personAvroType.schema
```

Which yields:

```json
{
  "name": "Person",
  "type": "record",
  "fields": [
    {"name": "name", "type": "string"},
    {"name": "age", "type": "int"}
  ],
  "namespace": "com.gensler.scalavro.tests"
}
```

And perhaps more interestingly:

```scala
case class SantaList(nice: Seq[Person], naughty: Seq[Person])

val santaListAvroType = AvroType[SantaList]
santaListAvroType.schema
```

Which yields:

```json
{
  "name": "SantaList",
  "type": "record",
  "fields": [
    {
      "name": "nice",
      "type": {"type": "array", "items": "Person"}
    },
    {
      "name": "naughty",
      "type": {"type": "array", "items": "Person"}
    }
  ],
  "namespace": "com.gensler.scalavro.tests"
}
```

<a name="supertypes-of-case-classes"></a>
#### From supertypes of case classes

Given:

```scala
class Alpha
abstract class Beta extends Alpha
case class Gamma() extends Alpha
case class Delta() extends Beta
case class Epsilon[T]() extends Beta
```

Usage:

```scala
import com.gensler.scalavro.AvroType
AvroType[Alpha].schema
```

Which yields:

```json
[
  {
    "name" : "Delta",
    "type" : "record",
    "fields" : [],
    "namespace" : "com.gensler.scalavro.tests"
  },
  {
    "name" : "Gamma",
    "type" : "record",
    "fields" : [],
    "namespace" : "com.gensler.scalavro.tests"
  }
]
```

Note that in the above example:

- `Alpha` is excluded from the union because it is not a case class
- `Beta` is excluded from the union because it is abstract and not a case class
- `Epsilon` is excluded from the union because it takes type parameters

<a name="binary-io"></a>
## Scalavro by Example: Binary IO

```scala
import com.gensler.scalavro.AvroType
import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.io.AvroTypeIO.Implicits._
import scala.util.{Try, Success, Failure}

case class Person(name: String, age: Int)
case class SantaList(nice: Seq[Person], naughty: Seq[Person])

val santaList = SantaList(
  nice = Seq(
    Person("John", 17),
    Person("Eve", 3)
  ),
  naughty = Seq(
    Person("Jane", 25),
    Person("Alice", 65)
  )
)

val santaListType = AvroType[SantaList]
val santaListIO = santaListType.io // implicitly: AvroTypeIO[SantaList]

val outStream: java.io.OutputStream = // some stream...

santaListIO.write(santaList, outStream)

val inStream: java.io.InputStream = // some stream...

santaListIO.read(inStream) match {
  case Success(readResult) => // readResult is an instance of SantaList
  case Failure(cause)      => // handle failure...
}
```

## Reference
1. [Current Apache Avro Specification](http://avro.apache.org/docs/current/spec.html)
1. [Scala 2.10 Reflection Overview](http://docs.scala-lang.org/overviews/reflection/overview.html)
1. [Great article on schema evolution in various serialization systems](http://martin.kleppmann.com/2012/12/05/schema-evolution-in-avro-protocol-buffers-thrift.html)
1. [Wickedly clever technique for representing unboxed union types, proposed by Miles Sabin](http://chuusai.com/2011/06/09/scala-union-types-curry-howard)

## Legal
Apache Avro is a trademark of The Apache Software Foundation.

Scalavro is distributed under the BSD 2-Clause License, the text of which follows:

Copyright (c) 2013, Gensler  
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

- Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.

- Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
