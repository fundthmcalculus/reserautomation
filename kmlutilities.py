import logging
from typing import List, Tuple, Any, Dict, Iterable

import geopy
import pykml
import pandas as pd


def get_kml_dataframe(doc, summary_columns: List[str]) -> pd.DataFrame:
    # Since KML is always a data table, every element has the same extended data
    base_columns = ['Trail Name', 'Length [miles]']
    base_columns.extend(get_extended_data(doc.Document.Placemark[0].ExtendedData).keys())
    data = pd.DataFrame(columns=base_columns)
    for placemark in doc.Document.Placemark:
        placemark_name = placemark.name.text.strip()
        logging.info(f'{placemark_name}')
        try:
            ext_data = get_extended_data(placemark.ExtendedData)

            coords, distance = get_coordinates(placemark)

            row = [placemark_name, distance]
            row.extend(ext_data.values())
            data = data.append(pd.DataFrame([row], columns=base_columns))
        except AttributeError as ae:
            # This skips the points and polygons
            pass

    # Summarize by type, status, and official
    for column in summary_columns:
        summary_pivottable(data, column)

    return data


def summary_pivottable(data: pd.DataFrame, pivot_column: str, data_column: str = 'Length [miles]') -> None:
    official = data.pivot_table(index=pivot_column, values=data_column, aggfunc='sum')
    official = official.rename({'': '[BLANK]'})
    print(official)
    logging.debug(official)


def save_dataframe(file_path: str, data: pd.DataFrame) -> None:
    data.to_csv(file_path, index=False)


def get_coordinates(placemark) -> Tuple[List[Tuple[float]], float]:
    coords = [geo_coords(c) for c in split_string(placemark.LineString.coordinates.text)]
    return coords, sum(map(get_miles, get_coord_pairs(coords)))


def parse_file(file_name: str):
    with open(file_name) as f:
        return pykml.parser.parse(f).getroot()


def geo_coords(s: str) -> Tuple[float]:
    return tuple(map(float, s.split(',')[:-1]))


def get_miles(x: Tuple[Tuple[float], Tuple[float]]) -> float:
    return geopy.distance.distance(*x).miles


def get_coord_pairs(x: List[Tuple[float]]) -> List[Tuple[Tuple[float], Tuple[float]]]:
    return list(zip(x[:-1], x[1:]))


def split_string(x: str) -> List[str]:
    return [line.strip() for line in x.split('\n') if line.strip()]


def get_extended_data(x) -> Dict[str, str]:
    return dict([(d.attrib.values()[0], csv_format_string((d.value.text or "").strip())) for d in x.Data])


def csv_format_string(x: str) -> str:
    return x.replace('\n', ' ').replace('\r', ' ').replace('\f', ' ').replace(',', '')
