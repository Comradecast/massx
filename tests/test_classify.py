from gva_pipeline.classify import classify_incident


def test_domestic_classification() -> None:
    text = (
        "Police said the shooting stemmed from a domestic violence dispute. "
        "The suspect opened fire after an argument with his estranged wife."
    )
    result = classify_incident(text)
    assert result.category == "domestic_family"
    assert result.confidence >= 0.88


def test_party_social_classification() -> None:
    text = (
        "Investigators said the gunfire erupted during a large birthday party. "
        "Dozens of people had gathered for the celebration."
    )
    result = classify_incident(text)
    assert result.category == "party_social_event"


def test_nightlife_classification() -> None:
    text = (
        "The shooting happened outside a downtown bar and nightclub district. "
        "Witnesses said patrons were leaving the lounge at closing time."
    )
    result = classify_incident(text)
    assert result.category == "nightlife_bar_district"
