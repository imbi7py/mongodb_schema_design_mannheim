import datetime
import sys

# noinspection PyPackageRequirements
import bson
import pymongo
import colors

slow_ms = 5


def remove_indexes(db):
    print(colors.bold_color + "Removing indexes ... ", end='')
    sys.stdout.flush()

    db.Publisher.drop_indexes()
    db.User.drop_indexes()
    db.Book.drop_indexes()
    print(colors.subdue_color + "done.")


def reset_profile_data(db):
    print(colors.bold_color + "Resetting profiling ... ", end='')
    sys.stdout.flush()

    db.set_profiling_level(pymongo.OFF)
    profile_coll = db.system.profile
    profile_coll.drop()
    print(colors.subdue_color + "done.")


def renable_profiling(db):
    print(colors.bold_color + "Enabling profiling for slow queries ... ", end='')
    sys.stdout.flush()

    db.set_profiling_level(pymongo.SLOW_ONLY, slow_ms=slow_ms)
    print(colors.subdue_color + "done.")


def query_data(db):
    print(colors.bold_color + "Running standard queries (generates profiling data) ... ", end='')
    sys.stdout.flush()

    users = db.User
    publishers = db.Publisher
    books = db.Book

    users.find({"Age": 18}).count()
    users.find_one({"UserId": 3})
    users.find({"Age": {'$gt': 61}}).count()
    users.find({"Location.City": "moscow"}).count()
    users.find({"Location.City": "moscow"}).count()
    p = publishers.find_one({"Name": "2nd Avenue Publishing, Inc."})
    d = list(books.find({"Publisher": p['_id']}, {"Title": 1}))[-1]
    d = datetime.datetime(2010, 1, 1)
    books.find({"Published": {"$gt": d}}).count()
    s = {r['Title'] for r in books.find({"Author": "John Grisham"}, {"Title": 1, "_id": 0})}
    d = datetime.datetime(2003, 1, 1)
    s = {r['Title'] for r in books.find({"Author": "John Grisham", "Published": {"$gt": d}},
                                        {"Title": 1, "_id": 0})}
    books.find({"Ratings.Value": {"$gte": 10}}).count()
    user_id = bson.ObjectId("525867733a93bb2198146309")
    books.find({"Ratings.UserId": user_id}).count()
    books.find({"Ratings.UserId": user_id, "Ratings.Value": {"$gte": 10}}).count()
    print(colors.subdue_color + "done.")


def display_bad_queries(db):
    print(colors.bold_color + "Displaying top 10 worst queries (slower than 5 ms) ...")

    queries = list(db.system.profile.find({"protocol": "op_query"}))
    print(colors.subdue_color + "Found {} bad queries".format(len(queries)))
    for q in queries:
        cmd = "unset"
        if q.get('op') == 'command':
            cmd = q.get('command', {'query': 'n/a'}).get('query')
        elif q.get('op') == 'query':
            cmd = q.get('query')
        print(colors.subdue_color + "time: ", end='')
        print(colors.highlight_color + '{:,}ms'.format(q['millis']), end='')
        print(colors.subdue_color + ", coll: {}, query: ".format(
            q['ns'].replace('books.', '')
        ), end='')
        print(colors.notice_color + '{}'.format(cmd))
    print(colors.subdue_color + "done.")


def add_indexes(db):
    print(colors.bold_color + "Adding indexes (this is s.l.o.w.) ...", end=' ')
    sys.stdout.flush()

    users = db.User
    publishers = db.Publisher
    books = db.Book

    books.create_index("Ratings.Value", name='books_by_rating_value')
    books.create_index([("Ratings.UserId", pymongo.ASCENDING),
                        ("Ratings.Value", pymongo.ASCENDING)],
                       name='books_by_rating_userid_and_value')
    books.create_index("Publisher", name="books_by_publisher")
    books.create_index("Published", name="books_by_published_date")
    books.create_index("Author", name="books_by_author")
    users.create_index("Age", name="users_by_age")
    users.create_index("UserId", name="users_by_id")
    users.create_index("Location.City", name="users_by_city")
    publishers.create_index("Name", name='publishers_by_name')

    print(colors.subdue_color + 'done.')


def display_times_and_query_plans(db, run_with_indexes):
    total_time = 0
    if run_with_indexes:
        print(colors.highlight_color + "Showing query times and plans with results (should be slow) ...")
    else:
        print(colors.highlight_color + "Showing query times and plans with results (should be faster) ...")

    users = db.User
    publishers = db.Publisher
    books = db.Book

    print(colors.subdue_color + "There are {:,} users who are 18 years old.".format(
        users.find({"Age": 18}).count()
    ))
    x = users.find({"Age": 18}).explain()
    total_time += print_explain_info(x, run_with_indexes)
    print()

    print(colors.subdue_color + "User with ID 3 is from {}".format(users.find_one({"UserId": 3})['Location']['City']))
    x = users.find({"UserId": 3}).explain()
    total_time += print_explain_info(x, run_with_indexes)
    print()

    print(colors.subdue_color + "There are {:,} users over 61".format(users.find({"Age": {'$gt': 61}}).count()))
    x = users.find({"Age": {'$gt': 61}}).explain()
    total_time += print_explain_info(x, run_with_indexes)
    print()

    c = users.find({"Location.City": "moscow"}).count()
    print(colors.subdue_color + "{:,} users are from Moscow".format(c))
    x = users.find({"Location.City": "moscow"}).explain()
    total_time += print_explain_info(x, run_with_indexes)
    print()

    p = publishers.find_one({"Name": "2nd Avenue Publishing, Inc."})
    print(colors.subdue_color + "2nd Avenue Publishing has ID {}".format(p.get('_id')))
    x = publishers.find({"Name": "2nd Avenue Publishing, Inc."}).explain()
    total_time += print_explain_info(x, run_with_indexes)
    print()

    ff = list(books.find({"Publisher": p['_id']}, {"Title": 1}))[-1]
    x = books.find({"Publisher": p['_id']}, {"Title": 1}).explain()
    print(colors.subdue_color + "Books published by 2nd Avenue Publishing, Inc.: {}".format(len(ff)))
    total_time += print_explain_info(x, run_with_indexes)
    print()

    d = datetime.datetime(2010, 1, 1)
    f = books.find({"Published": {"$gt": d}}).count()
    print(colors.subdue_color + "{:,} books have been published since 2001".format(f))
    x = books.find({"Published": {"$gt": d}}).explain()
    total_time += print_explain_info(x, run_with_indexes)
    print()

    ff = {r['Title'] for r in books.find({"Author": "John Grisham"}, {"Title": 1, "_id": 0})}
    print(colors.subdue_color + "Books written by John Grisham, count: {}".format(len(ff)))
    print(ff)
    x = books.find({"Author": "John Grisham"}, {"Title": 1, "_id": 0}).explain()
    total_time += print_explain_info(x, run_with_indexes)
    print()

    d = datetime.datetime(2003, 1, 1)
    ff = {r['Title'] for r in books.find({"Author": "John Grisham", "Published": {"$gt": d}},
                                         {"Title": 1, "_id": 0})}
    x = books.find({"Author": "John Grisham", "Published": {"$gt": d}},
                   {"Title": 1, "_id": 0}).explain()
    print(colors.subdue_color + "Books written by John Grisham after 2003, count: {}".format(len(ff)))
    print(ff)
    total_time += print_explain_info(x, run_with_indexes)
    print()

    ff = books.find({"Ratings.Value": {"$gte": 10}}).count()
    print(colors.subdue_color + "Books rated 'perfect 10': {:,}".format(ff))
    x = books.find({"Ratings.Value": {"$gte": 10}}).explain()
    total_time += print_explain_info(x, run_with_indexes)
    print()

    user_id = bson.ObjectId("525867733a93bb2198146309")
    ff = books.find({"Ratings.UserId": user_id}).count()
    print(colors.subdue_color + "Books rated by particular user {}".format(ff))
    x = books.find({"Ratings.UserId": user_id}).explain()
    total_time += print_explain_info(x, run_with_indexes)
    print()

    ff = books.find({"Ratings.UserId": user_id, "Ratings.Value": {"$gte": 10}}).count()
    print(colors.subdue_color + "Boos rated perfect 10 by {}: {}".format(user_id, ff))
    x = books.find({"Ratings.UserId": user_id, "Ratings.Value": {"$gte": 10}}).explain()
    total_time += print_explain_info(x, run_with_indexes)
    print()

    print(colors.subdue_color + "done, total time: ", end='')
    print(colors.highlight_color + '{:,}'.format(total_time), end='')
    print(colors.subdue_color + ' ms.')


def print_explain_info(explain_object, run_with_indexes):
    x = explain_object
    time_ms = 'unknown'
    execution_stats = x.get('executionStats')
    if execution_stats:
        execution_time_ms = execution_stats.get('executionTimeMillis')
        if execution_time_ms is not None:
            time_ms = execution_time_ms

    index_name = "NO INDEX"
    has_index = False
    non_index_stage = 'UNKNOWN_STATUS'
    query_planner = x.get('queryPlanner')
    if query_planner:
        winning_plan = query_planner.get('winningPlan')
        if winning_plan:
            input_stage = winning_plan.get('inputStage', {'indexName': None})
            index_name = input_stage.get('indexName')
            if not index_name:
                input_stage = input_stage.get('inputStage', {'indexName': None})
                index_name = input_stage.get('indexName')
                if not index_name:
                    non_index_stage = winning_plan.get('stage')
                else:
                    has_index = True
            else:
                has_index = True

    if has_index:
        print(colors.notice_color + "INDEXED", end='')
        print(colors.subdue_color + ": time: ", end='')
        print(colors.highlight_color + '{} ms'.format(time_ms), end='')
        print(colors.subdue_color + ', index: ', end='')
        print(colors.highlight_color + "{}".format(index_name))
    else:
        print(colors.notice_color + "No index", end='')
        print(colors.subdue_color + ": time: ", end='')
        print(colors.highlight_color + '{} ms'.format(time_ms), end='')
        print(colors.subdue_color + ', index: {}'.format(index_name), end='')
        print(colors.subdue_color + ', stage: ', end='')
        print(colors.highlight_color + "{}".format(non_index_stage))
        # if run_with_indexes:
        #     print(pprint.pprint(x))

    return time_ms


def show_data_size(db):
    print(colors.bold_color + "Computing data set size...")
    sys.stdout.flush()
    print(colors.highlight_color + "{:,}".format(db.Book.count()),end='')
    print(colors.subdue_color + ' books')
    sys.stdout.flush()

    review_count = 0
    for b in db.Book.find({}, {'_id': 0, 'Ratings.Value': 1}):
        review_count += len(b['Ratings'])
    print(colors.highlight_color + "{:,}".format(review_count),end='')
    print(colors.subdue_color + " reviews")
    print()


def run():
    client = pymongo.MongoClient()
    db = client.books

    show_data_size(db)
    remove_indexes(db)
    reset_profile_data(db)
    renable_profiling(db)
    query_data(db)
    input(colors.subdue_color + "--------- paused - enter to see bad queries -----")
    display_bad_queries(db)
    input(colors.subdue_color + "--------- paused - enter to display times -------")
    display_times_and_query_plans(db, False)
    input(colors.subdue_color + "--------- paused - enter to add indexes ---------")
    add_indexes(db)
    reset_profile_data(db)
    renable_profiling(db)
    query_data(db)
    display_bad_queries(db)
    input(colors.subdue_color + "--------- paused - enter to display times -------")
    display_times_and_query_plans(db, True)
    print()
