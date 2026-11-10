from ask_lakehouse.execute import schema_problems


def test_semantic_layer_matches_database_schema(db_path, semantic):
    assert schema_problems(db_path, semantic) == []


def test_semantic_render_mentions_every_table_and_metric(semantic):
    text = semantic.render()
    for table in semantic.table_names:
        assert table in text
    for metric in semantic.raw["metrics"]:
        assert metric["name"] in text


def test_demo_data_is_consistent_across_tables(executor):
    zone_day = executor.run("SELECT SUM(trips), SUM(revenue) FROM trips_by_zone_day").rows[0]
    hourly = executor.run("SELECT SUM(trips), SUM(revenue) FROM revenue_by_hour").rows[0]
    assert zone_day[0] == hourly[0]
    assert abs(zone_day[1] - hourly[1]) / zone_day[1] < 1e-5


def test_coverage_describes_the_data(executor):
    cov = executor.coverage()
    assert cov["first_date"] == "2024-01-01" and cov["last_date"] == "2024-06-30"
    assert cov["months"][0] == "2024-01" and cov["months"][-1] == "2024-06"
