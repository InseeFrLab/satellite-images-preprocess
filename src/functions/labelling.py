from classes.labelers.labeler import BDTOPOLabeler, COSIALabeler


def get_labeler(type_labeler, year, dep, task):
    """
    Get a labeler instance based on the specified type.

    Parameters:
    - type_labeler (str): The type of labeler to instantiate.
    - year (int): The year parameter for labeler instantiation.
    - dep (str): The dep parameter for labeler instantiation.
    - task (str): The task parameter for labeler instantiation.


    Returns:
    - Labeler: An instance of the specified labeler type.
    """
    labeler = None

    match type_labeler:
        case "BDTOPO":
            labeler = BDTOPOLabeler(year=year, dep=dep, task=task)
        case "COSIA":
            labeler = COSIALabeler(year=year, dep=dep, task=task)
        case _:
            pass
    return labeler
