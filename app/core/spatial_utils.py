"""
Advanced spatial query utilities with proper geometric calculations
"""

from typing import List, Tuple
import math


def point_in_polygon(
    point: Tuple[float, float], polygon: List[Tuple[float, float]]
) -> bool:
    """
    Determine if a point is inside a polygon using the ray casting algorithm.

    Args:
        point: (latitude, longitude) tuple
        polygon: List of (latitude, longitude) tuples defining polygon vertices

    Returns:
        bool: True if point is inside polygon, False otherwise
    """
    if not polygon or len(polygon) < 3:
        return False

    x, y = point
    n = len(polygon)
    inside = False

    p1x, p1y = polygon[0]
    for i in range(1, n + 1):
        p2x, p2y = polygon[i % n]
        if y > min(p1y, p2y):
            if y <= max(p1y, p2y):
                if x <= max(p1x, p2x):
                    if p1y != p2y:
                        xinters = (y - p1y) * (p2x - p1x) / (p2y - p1y) + p1x
                    if p1x == p2x or x <= xinters:
                        inside = not inside
        p1x, p1y = p2x, p2y

    return inside


def calculate_polygon_bounds(
    polygon: List[Tuple[float, float]],
) -> Tuple[float, float, float, float]:
    """
    Calculate bounding box for a polygon.

    Args:
        polygon: List of (latitude, longitude) tuples

    Returns:
        tuple: (min_lat, min_lng, max_lat, max_lng)
    """
    if not polygon:
        return 0.0, 0.0, 0.0, 0.0

    lats = [p[0] for p in polygon]
    lngs = [p[1] for p in polygon]

    return min(lats), min(lngs), max(lats), max(lngs)


def haversine_distance(
    lat1: float, lng1: float, lat2: float, lng2: float
) -> float:
    """
    Calculate the great circle distance between two points on Earth using the Haversine formula.

    Args:
        lat1, lng1: Latitude and longitude of first point in decimal degrees
        lat2, lng2: Latitude and longitude of second point in decimal degrees

    Returns:
        float: Distance in kilometers
    """
    # Convert to radians
    lat1, lng1, lat2, lng2 = map(math.radians, [lat1, lng1, lat2, lng2])

    # Haversine formula
    dlat = lat2 - lat1
    dlng = lng2 - lng1
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlng / 2) ** 2
    )
    c = 2 * math.asin(math.sqrt(a))

    # Radius of Earth in kilometers
    r = 6371

    return c * r


def expand_bounding_box(
    min_lat: float,
    min_lng: float,
    max_lat: float,
    max_lng: float,
    buffer_km: float = 10.0,
) -> Tuple[float, float, float, float]:
    """
    Expand a bounding box by a buffer distance in kilometers.

    Args:
        min_lat, min_lng, max_lat, max_lng: Original bounding box
        buffer_km: Buffer distance in kilometers

    Returns:
        tuple: Expanded bounding box (min_lat, min_lng, max_lat, max_lng)
    """
    # Approximate km per degree (varies by latitude)
    km_per_degree_lat = 111.0

    # Longitude km per degree varies by latitude
    center_lat = (min_lat + max_lat) / 2
    km_per_degree_lng = 111.0 * math.cos(math.radians(center_lat))

    # Calculate buffer in degrees
    lat_buffer = buffer_km / km_per_degree_lat
    lng_buffer = buffer_km / km_per_degree_lng if km_per_degree_lng > 0 else 0

    return (
        max(min_lat - lat_buffer, -90),  # Don't go below -90
        max(min_lng - lng_buffer, -180),  # Don't go below -180
        min(max_lat + lat_buffer, 90),  # Don't go above 90
        min(max_lng + lng_buffer, 180),  # Don't go above 180
    )


def filter_companies_by_polygon(
    companies: List[dict], polygon: List[Tuple[float, float]]
) -> List[dict]:
    """
    Filter a list of companies to only those inside a polygon.

    Args:
        companies: List of company dicts with 'latitude' and 'longitude' keys
        polygon: List of (latitude, longitude) tuples defining polygon

    Returns:
        List[dict]: Filtered companies inside the polygon
    """
    if not polygon or len(polygon) < 3:
        return companies

    filtered = []
    for company in companies:
        lat = company.get("latitude")
        lng = company.get("longitude")

        if lat is not None and lng is not None:
            try:
                lat_float = float(lat)
                lng_float = float(lng)

                if point_in_polygon((lat_float, lng_float), polygon):
                    # Add distance from polygon center for sorting
                    center_lat = sum(p[0] for p in polygon) / len(polygon)
                    center_lng = sum(p[1] for p in polygon) / len(polygon)

                    distance = haversine_distance(
                        lat_float, lng_float, center_lat, center_lng
                    )
                    company_copy = company.copy()
                    company_copy["distance_km"] = round(distance, 2)
                    filtered.append(company_copy)

            except (ValueError, TypeError):
                # Skip companies with invalid coordinates
                continue

    # Sort by distance from polygon center
    filtered.sort(key=lambda x: x.get("distance_km", float("inf")))

    return filtered


def validate_polygon_coordinates(
    coordinates: List[List[float]],
) -> List[Tuple[float, float]]:
    """
    Validate and convert polygon coordinates.

    Args:
        coordinates: List of [lat, lng] coordinate pairs

    Returns:
        List[Tuple[float, float]]: Validated polygon coordinates

    Raises:
        ValueError: If coordinates are invalid
    """
    if not coordinates or len(coordinates) < 3:
        raise ValueError("Polygon must have at least 3 coordinates")

    if len(coordinates) > 1000:
        raise ValueError("Polygon cannot have more than 1000 coordinates")

    validated = []
    for i, coord in enumerate(coordinates):
        if not isinstance(coord, (list, tuple)) or len(coord) != 2:
            raise ValueError(f"Coordinate {i} must be [latitude, longitude]")

        try:
            lat, lng = float(coord[0]), float(coord[1])
        except (TypeError, ValueError):
            raise ValueError(f"Coordinate {i} must contain valid numbers")

        if not (-90 <= lat <= 90):
            raise ValueError(
                f"Coordinate {i} latitude must be between -90 and 90"
            )

        if not (-180 <= lng <= 180):
            raise ValueError(
                f"Coordinate {i} longitude must be between -180 and 180"
            )

        validated.append((lat, lng))

    # Check if polygon is closed (first and last points are the same)
    if validated[0] != validated[-1]:
        validated.append(validated[0])  # Close the polygon

    return validated
