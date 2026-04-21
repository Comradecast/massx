from gva_pipeline.demographics import extract_suspect_demographics


def test_explicit_age_extraction() -> None:
    text = (
        "Police said the suspect, a 24-year-old man, was arrested at the scene after the shooting."
    )
    result = extract_suspect_demographics(text)
    assert result.suspect_age == 24
    assert result.suspect_age_confidence > 0.9


def test_ambiguous_gender_not_extracted() -> None:
    text = (
        "A woman and two children were hurt in the shooting. "
        "Police said the suspect fled before officers arrived."
    )
    result = extract_suspect_demographics(text)
    assert result.suspect_gender == "unknown"


def test_race_left_unknown_unless_explicit() -> None:
    text = (
        "Police said the suspect, a 32-year-old man, was arrested after the shooting. "
        "No additional description was released."
    )
    result = extract_suspect_demographics(text)
    assert result.suspect_race == "unknown"
