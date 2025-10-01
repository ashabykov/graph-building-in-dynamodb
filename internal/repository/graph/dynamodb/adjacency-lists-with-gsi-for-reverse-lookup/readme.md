# Adjacency list design pattern

When different entities of an application have a many-to-many relationship between them, the relationship can be modeled
as an adjacency list. In this pattern, all top-level entities (synonymous to nodes in the graph model) are represented
using the partition key. Any relationships with other entities (edges in a graph) are represented as an item within the
partition by setting the value of the sort key to the target entity ID (target node).

based on article: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-adjacency-graphs.html
Example: https://repost.aws/articles/ARs-sKseqITnWrHjMvYzLk7w/dynamodb-single-table-design-building-a-many-to-many-relationship-model-using-adjacency-lists