# MongoDB Schema Design - Mannheim MUG

This repository is the demo code from my May 3, 2016 presentation at [MongoDB User Group](http://www.meetup.com/Mannheim-MongoDB-User-Group/events/228698842/) in Mannheim, Germany.

The code here represents two things.

1. Two simple document modeling options for modeling a bookstore with books and reviews. [`book_normalized.js`](https://github.com/mikeckennedy/mongodb_schema_design_mannheim/raw/master/data/json_examples/book_normalized.js) shows creating two collections with weak foreign keys and [`book_embedded.js`](https://github.com/mikeckennedy/mongodb_schema_design_mannheim/raw/master/data/json_examples/book_embedded.js) shows an embedded subdocument.

2. An application using the database (`mongorestore` files [here](https://github.com/mikeckennedy/mongodb_schema_design_mannheim/raw/master/data/books.zip)) exploring the performance of querying large quantities of data (250K books with 1.2M nested reviews) and the influence of indexes.

Python application here:

[mongodb_schema_design_mannheim/tree/master/src/program.py](https://github.com/mikeckennedy/mongodb_schema_design_mannheim/tree/master/src)

You should see something along the lines of this output:

![](https://raw.githubusercontent.com/mikeckennedy/mongodb_schema_design_mannheim/master/data/readme_resources/term.png)



