from plaita.core.expression_parser import ExpressionParser


def test_get_registered_names_sorted_nonempty():
    names = ExpressionParser.get_registered_names()
    assert isinstance(names, list)
    assert names
    assert names == sorted(names)
    assert "add" in names
