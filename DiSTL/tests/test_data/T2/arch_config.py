# parameters
params = {"db": "DJWSJ",
          "model_name": "T2",
          "project_dir": "/home/lbybee/Documents/repos/github/DiSTL/DiSTL/tests/test_data/T2",
          "article_bottom_thresh": 0.0,
          "article_top_thresh": 1.0,
          "tfidf_thresh": 0.0,
          "stem": True}

# pipeline for DTM aggregation
dtm_pipeline = [{"$limit": 2500},
                {"$unwind": "$txt"},
                 {"$addFields": {"day-date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$display-date"}}}},
                {"$group": {"_id": {"display-date": "$day-date",
                                    "txt": "$txt"},
                            "count": {"$sum": 1}}},
                {"$addFields": {"display-date": "$_id.display-date"}},
                {"$group": {"_id": "$display-date",
                            "terms": {"$push": {"term": "$_id.txt",
                                                "count": "$count"}}}}]

# pipeline for vocab mongodb aggregation
vocab_doc_count_pipeline = [{"$limit": 2500},
                            {"$unwind": "$txt"},
                            {"$addFields": {"day-date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$display-date"}}}},
                            {"$group": {"_id": {"day-date": "$day-date",
                                                "txt": "$txt"},
                                        "count": {"$sum": 1}}},
                             {"$group": {"_id": "$_id.txt",
                                         "count": {"$sum": 1}}}]

# collection generator
def collection_gen():

    for y in range(2017, 2018):
        yield "y%d" % y

# count method
def count_method(db, collection, client):

    pipeline = [{"$limit": 2500},
                {"$addFields": {"day-date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$display-date"}}}},
                {"$group": {"_id": "$day-date"}}]
    iterator = client[db][collection].aggregate(pipeline, allowDiskUse=True)
    return len(list(iterator))
