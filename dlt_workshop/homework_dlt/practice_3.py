import dlt

def question_4():
    pipeline = dlt.pipeline(pipeline_name="dlt_practice", destination='duckdb', dataset_name='dlt')

    def people_1():
        for i in range(1, 6):
            yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}

    def people_2():
        for i in range(3, 9):
            yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}

    generator = people_1()
    for chunk in generator:
        pipeline.run(   [chunk],
                        table_name="contact_list_merged",
                        write_disposition="merge",
                        primary_key="ID")
        
    generator = people_2()
    for chunk in generator:
        pipeline.run(   [chunk],
                        table_name="contact_list_merged",
                        write_disposition="merge",
                        primary_key="ID")



