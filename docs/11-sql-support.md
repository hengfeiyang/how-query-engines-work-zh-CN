# SQL 支持

_本章所讨论的源代码可以在 [KQuery 项目](https://github.com/andygrove/how-query-engines-work) 的 `sql` 模块中找到。_

除了能够手写逻辑计划之外，在某些情况下只编写 SQL 会更方便。在本章中，我们将构建一个 SQL 解析器和查询计划器，可以将 SQL 查询转换为逻辑计划。

## 分词器 Tokenizer

第一步是将 SQL 查询字符串转换为表示关键字、字面量、标识符和运算符的标记列表。

这只是所有可能标记的子集，但现在已经足够了。

```kotlin
interface Token
data class IdentifierToken(val s: String) : Token
data class LiteralStringToken(val s: String) : Token
data class LiteralLongToken(val s: String) : Token
data class KeywordToken(val s: String) : Token
data class OperatorToken(val s: String) : Token
```

然后我们需要一个分词器类。在这里没有特别必要完整的过一遍，完整的源代码可以在配套的 GitHub 代码库中找到。

```kotlin
class Tokenizer {
  fun tokenize(sql: String): List<Token> {
    // see github repo for code
  }
}
```

给定输入 `"SELECT a + b FROM c"` 我们期望输出如下：

```kotlin
listOf(
  KeywordToken("SELECT"),
  IdentifierToken("a"),
  OperatorToken("+"),
  IdentifierToken("b"),
  KeywordToken("FROM"),
  IdentifierToken("c")
)
```

## Pratt 解析器

我们将根据 Vaughan R. Pratt 于 1973 年发表的 [Top Down Operator Precedence](https://tdop.github.io/) 论文手动编写 SQL 解析器。尽管还有其他方法来构建 SQL 解析器，例如使用解析器生成器和解析器组合器，但我发现了 Pratt 的方法工作良好，代码高效、易于理解且易于调试。

这是 Pratt 解析器的简单实现。在我看来，它的美丽在于它的简单。表达式解析是通过一个简单的循环来执行的，该循环解析“前缀”表达式，后跟一个可选的“中缀”表达式，并继续执行此操作，直到优先级发生变化，使解析器认识到它已完成对表达式的解析。当然，`parsePrefix` 和 `parseInfix` 的实现可以递归地回调  `parse` 方法，这就是它变得非常强大的地方。

```kotlin
interface PrattParser {

  /** Parse an expression */
  fun parse(precedence: Int = 0): SqlExpr? {
    var expr = parsePrefix() ?: return null
    while (precedence < nextPrecedence()) {
      expr = parseInfix(expr, nextPrecedence())
    }
    return expr
  }

  /** Get the precedence of the next token */
  fun nextPrecedence(): Int

  /** Parse the next prefix expression */
  fun parsePrefix(): SqlExpr?

  /** Parse the next infix expression */
  fun parseInfix(left: SqlExpr, precedence: Int): SqlExpr

}
```

该接口引用一个新 `SqlExpr` 类，它将作为解析表达式的表示，并且很大程度上会与逻辑计划中定义的表达式一一对应，但对于二元表达式，我们可以使用更通用的结构，其中运算符是字符串而不是为所有不同二元表达式创建单独数据结构。

以下是一些实现 `SqlExpr` 的例子。

```kotlin
/** SQL Expression */
interface SqlExpr

/** Simple SQL identifier such as a table or column name */
data class SqlIdentifier(val id: String) : SqlExpr {
  override fun toString() = id
}

/** Binary expression */
data class SqlBinaryExpr(val l: SqlExpr, val op: String, val r: SqlExpr) : SqlExpr {
  override fun toString(): String = "$l $op $r"
}

/** SQL literal string */
data class SqlString(val value: String) : SqlExpr {
  override fun toString() = "'$value'"
}
```

有了这些类后，就可以用下面代码表示 `foo = 'bar'`。

```kotlin
val sqlExpr = SqlBinaryExpr(SqlIdentifier("foo"), "=", SqlString("bar"))
```

## 解析 SQL 表达式

让我们通过解析一个简单的数学表达式 `1 + 2 * 3` 来逐步了解这种方法，该表达式由以下标记组成。

```kotlin
listOf(
  LiteralLongToken("1"),
  OperatorToken("+"),
  LiteralLongToken("2"),
  OperatorToken("*"),
  LiteralLongToken("3")
)
```

我们需要创建一个实现了 `PrattParser` 特性（trait） 的对象，并将标记（tokens）传入构造函数。这些标记（tokens）被包装在一个 `TokenStream` 类中，该类提供了一些方便的方法，例如`next` 用于消耗下一个标记（tokens），以及 `peek` 当我们想要向前查看而不消耗标记（token）时。

```kotlin
class SqlParser(val tokens: TokenStream) : PrattParser {
}
```

实现 `nextPrecedence` 方法很简单，因为我们只有少量具有优先级的标记（token），我们需要让`乘法` 和 `除法` 运算符具有比 `加法` 和 `减法` 运算符更高的优先级。请注意，此方法返回的具体数字并不重要，因为它们仅用于比较。关于运算符优先级的一个很好的参考可以在 [PostgreSQL documentation](https://www.postgresql.org/docs/7.2/sql-precedence.html) 中找到。

```kotlin
override fun nextPrecedence(): Int {
  val token = tokens.peek()
  return when (token) {
    is OperatorToken -> {
      when (token.s) {
        "+", "-" -> 50
        "*", "/" -> 60
        else -> 0
      }
    }
    else -> 0
  }
}
```

前缀解析器只需要知道如何解析字面数值。

```kotlin
override fun parsePrefix(): SqlExpr? {
  val token = tokens.next() ?: return null
  return when (token) {
    is LiteralLongToken -> SqlLong(token.s.toLong())
    else -> throw IllegalStateException("Unexpected token $token")
  }
}
```

中缀解析器只需要知道如何解析运算符。请注意，解析运算符后，此方法会递归回调上层的 `parse` 方法来解析运算符后面的表达式（二元表达式的右侧）。

```kotlin
override fun parseInfix(left: SqlExpr, precedence: Int): SqlExpr {
  val token = tokens.peek()
  return when (token) {
    is OperatorToken -> {
      tokens.next()
      SqlBinaryExpr(left, token.s, parse(precedence) ?:
                    throw SQLException("Error parsing infix"))
    }
    else -> throw IllegalStateException("Unexpected infix token $token")
  }
}
```

优先级逻辑可以通过解析数学表达式 `1 + 2 * 3` 和 `1 * 2 + 3` 来演示，它们应该被分别解析为 `1 + (2 * 3)` 和 `(1 * 2) + 3`。

*示例: 解析 `1 + 2 _ 3`*

这些是标记及其优先级值。

```
Tokens:      [1]  [+]  [2]  [*]  [3]
Precedence:  [0] [50]  [0] [60]  [0]
```

最终结果正确地将表达式表示为 `1 + (2 * 3)`。

```kotlin
SqlBinaryExpr(
    SqlLong(1),
    "+",
    SqlBinaryExpr(SqlLong(2), "*", SqlLong(3))
)
```

*示例: 解析 `1 _ 2 + 3`*

```
Tokens:      [1]  [*]  [2]  [+]  [3]
Precedence:  [0] [60]  [0] [50]  [0]
```

最终结果正确地将表达式表示为 `(1 * 2) + 3`。

```kotlin
SqlBinaryExpr(
    SqlBinaryExpr(SqlLong(1), "*", SqlLong(2)),
    "+",
    SqlLong(3)
)
```

## 解析 SELECT 语句

现在我们已经能够解析一些简单的表达式，下一步是扩展解析器以支持将 SELECT 语句解析为具体语法树 (CST)。请注意，使用其他解析方法（例如使用像 ANTLR 这样的解析器生成器）时，有一个称为抽象语法树（AST）的中间阶段，然后需要将其转换为具体语法树，但使用 Pratt 解析器方法，我们直接从标记（tokens）生成 CST。

下面是一个示例 CST，它可以表示带有映射（projection）和筛选（selection）的简单单表查询。将在后面的章节中扩展以支持更复杂的查询。

```kotlin
data class SqlSelect(
    val projection: List<SqlExpr>,
    val selection: SqlExpr,
    val tableName: String) : SqlRelation
```

## SQL 查询规划器 Query Planner

SQL 查询规划器将 SQL 查询树转换为逻辑计划。由于 SQL 语言的灵活性，这比将逻辑计划转换为物理计划要困难得多。例如，考虑以下简单查询。

```sql
SELECT id, first_name, last_name, salary/12 AS monthly_salary
FROM employee
WHERE state = 'CO' AND monthly_salary > 1000
```

尽管这对于阅读查询的人来说很直观，但查询的选择部分（`WHERE`子句）引用了一个不包含在映射（selection）输出中的表达式（`state`），因此显然需要在投影之前应用它，同时还引用另一个表达式（`salary/12 AS monthly_salary`) 仅在应用映射后才可用。我们还会遇到类似 `GROUP BY`, `HAVING`, 和 `ORDER BY` 子句等问题。

这个问题有多种解决方案。一种方法是将此查询转换为以下逻辑计划，将选择表达式分为两个步骤，一个在映射之前，一个在映射之后。然而，只有当选择表达式是连接性断言（conjunctive predicate）（即所有部分都为真时，表达式才为真）时这种方法才可能行得通，对于更复杂的表达式来说，这种方法可能就不适用了。如果表达式是 `state = 'CO' OR monthly_salary > 1000` 那么我们就不能这样做。

```
Filter: #monthly_salary > 1000
  Projection: #id, #first_name, #last_name, #salary/12 AS monthly_salary
    Filter: #state = 'CO'
      Scan: table=employee
```

一种更简单、更通用的方法是将所有必需的表达式添加到映射（projection），以便可以在映射之后应用选择（selection），然后删除 通过将输出包装在另一个映射中而添加的列。

```
Projection: #id, #first_name, #last_name, #monthly_salary
  Filter: #state = 'CO' AND #monthly_salary > 1000
    Projection: #id, #first_name, #last_name, #salary/12 AS monthly_salary, #state
      Scan: table=employee
```

值得注意的是，我们将在后面的章节中构建一个“断言下推（Predicate Push Down）”查询优化器规则，该规则将能够优化该计划并将 `state = 'CO'` 断言部分在计划中进一步下推，使其位于映射之前。

## 转换 SQL 表达式

将 SQL 表达式转换为逻辑表达式相当简单，如本示例代码所示。

```kotlin
private fun createLogicalExpr(expr: SqlExpr, input: DataFrame) : LogicalExpr {
  return when (expr) {
    is SqlIdentifier -> Column(expr.id)
    is SqlAlias -> Alias(createLogicalExpr(expr.expr, input), expr.alias.id)
    is SqlString -> LiteralString(expr.value)
    is SqlLong -> LiteralLong(expr.value)
    is SqlDouble -> LiteralDouble(expr.value)
    is SqlBinaryExpr -> {
      val l = createLogicalExpr(expr.l, input)
      val r = createLogicalExpr(expr.r, input)
      when(expr.op) {
        // comparison operators
        "=" -> Eq(l, r)
        "!=" -> Neq(l, r)
        ">" -> Gt(l, r)
        ">=" -> GtEq(l, r)
        "<" -> Lt(l, r)
        "<=" -> LtEq(l, r)
        // boolean operators
        "AND" -> And(l, r)
        "OR" -> Or(l, r)
        // math operators
        "+" -> Add(l, r)
        "-" -> Subtract(l, r)
        "*" -> Multiply(l, r)
        "/" -> Divide(l, r)
        "%" -> Modulus(l, r)
        else -> throw SQLException("Invalid operator ${expr.op}")
      }
    }

    else -> throw new UnsupportedOperationException()
  }
}
```

## 规划选择

如果我们只想支持所有在选择中引用的列也存在于映射中的用例，我们可以通过一些非常简单的逻辑来构建查询计划。

```kotlin
fun createDataFrame(select: SqlSelect, tables: Map<String, DataFrame>) : DataFrame {

  // get a reference to the data source
  var df = tables[select.tableName] ?:
      throw SQLException("No table named '${select.tableName}'")

  val projectionExpr = select.projection.map { createLogicalExpr(it, df) }

  if (select.selection == null) {
    // apply projection
    return df.select(projectionExpr)
  }

  // apply projection then wrap in a selection (filter)
  return df.select(projectionExpr)
           .filter(createLogicalExpr(select.selection, df))
}
```

然而，由于选择可能同时引用映射的输入和输出，因此我们需要创建一个更复杂的带有中间投影的计划。第一步是确定哪些列被选择过滤表达式引用。为此，我们将使用访问者模式来遍历表达式树并构建一个可变的列名称集合。

以下是我们将用来遍历表达式树的实用方法。

```kotlin
private fun visit(expr: LogicalExpr, accumulator: MutableSet<String>) {
  when (expr) {
    is Column -> accumulator.add(expr.name)
    is Alias -> visit(expr.expr, accumulator)
    is BinaryExpr -> {
      visit(expr.l, accumulator)
      visit(expr.r, accumulator)
     }
  }
}
```

有了这个，我们现在可以编写以下代码将 SELECT 语句转换为有效的逻辑计划。此代码示例并不完美，可能包含一些边缘情况的错误，其中数据源中的列和别名表达式之间存在名称冲突，但是为了保持代码简洁暂时忽略它们。

```kotlin
fun createDataFrame(select: SqlSelect, tables: Map<String, DataFrame>) : DataFrame {

  // get a reference to the data source
  var df = tables[select.tableName] ?:
    throw SQLException("No table named '${select.tableName}'")

  // create the logical expressions for the projection
  val projectionExpr = select.projection.map { createLogicalExpr(it, df) }

  if (select.selection == null) {
    // if there is no selection then we can just return the projection
    return df.select(projectionExpr)
  }

  // create the logical expression to represent the selection
  val filterExpr = createLogicalExpr(select.selection, df)

  // get a list of columns references in the projection expression
  val columnsInProjection = projectionExpr
    .map { it.toField(df.logicalPlan()).name}
    .toSet()

  // get a list of columns referenced in the selection expression
  val columnNames = mutableSetOf<String>()
  visit(filterExpr, columnNames)

  // determine if the selection references any columns not in the projection
  val missing = columnNames - columnsInProjection

  // if the selection only references outputs from the projection we can
  // simply apply the filter expression to the DataFrame representing
  // the projection
  if (missing.size == 0) {
    return df.select(projectionExpr)
             .filter(filterExpr)
  }

  // because the selection references some columns that are not in the
  // projection output we need to create an interim projection that has
  // the additional columns and then we need to remove them after the
  // selection has been applied
  return df.select(projectionExpr + missing.map { Column(it) })
           .filter(filterExpr)
           .select(projectionExpr.map {
              Column(it.toField(df.logicalPlan()).name)
            })
}
```

## 规划聚合查询

如你所见，SQL查询规划器相对复杂，解析聚合查询的代码也颇多。如果你对此感兴趣，请参阅源代码。

*这本书还可通过 [https://leanpub.com/how-query-engines-work](https://leanpub.com/how-query-engines-work) 购买 ePub、MOBI 和 PDF格式版本。*

**Copyright © 2020-2023 Andy Grove. All rights reserved.**
