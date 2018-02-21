"""Module implementing classes common to all modules of the package (such as the bounding box class).

Representing bounding box can be confusing. If a function expects bbox as a list of four coordinates,
is it [lat1, lon1, lat2, lon2]? Or is it something else? And what CRS does it expect? Is the CRS a
separate parameter?

In this module the BBox class provides the canonical representation of a BBox that all the functions and
classes of the sg_utils package use, solving these issues.

Available classes:
 - BBox, represent a bounding box in a given CRS
"""

from .constants import CRS


class BBox:
    """ Class representing a bounding box in a given CRS.

    Throughout the sentinelhub package this class serves as the canonical representation of a bounding
    box. It can instantiate itself from multiple representations:

        1) ``((min_x,min_y),(max_x,max_y))``,
        2) ``(min_x,min_y,max_x,max_y)``,
        3) ``[min_x,min_y,max_x,max_y]``,
        4) ``[[min_x, min_y],[max_x,max_y]]``,
        5) ``[(min_x, min_y),(max_x,max_y)]``,
        6) ``([min_x, min_y],[max_x,max_y])``,
        7) ``'min_x,min_y,max_x,max_y'``,
        8) ``{'min_x':min_x, 'max_x':max_x, 'min_y':min_y, 'max_y':max_y}``,
        9) ``bbox``, where ``bbox`` is an instance of ``BBox``.

    Note that BBox coordinate system depends on ``crs`` parameter:
        - In case of ``constants.CRS.WGS84`` axis x represents latitude and axis y represents longitude
        - In case of ``constants.CRS.POP_WEB`` axis x represents easting and axis y represents northing
        - In case of ``constants.CRS.UTM_*`` axis x represents easting and axis y represents northing

    :param bbox: a bbox in a number of representations.
    :param crs: Coordinate Reference System that bbox is in. Expect one of the constants from the ``const.CRS`` enum.
    :type crs: constants.CRS
    """
    def __init__(self, bbox, crs):
        x_fst, y_fst, x_snd, y_snd = BBox._to_tuple(bbox)
        self.min_x = min(x_fst, x_snd)
        self.max_x = max(x_fst, x_snd)
        self.min_y = min(y_fst, y_snd)
        self.max_y = max(y_fst, y_snd)
        self.crs = CRS(crs)

    def __iter__(self):
        return iter(self.get_lower_left() + self.get_upper_right())

    def get_lower_left(self):
        """ Returns the lower left vertex of the bounding box

        :return: min_x, min_y
        :rtype: (float, float)
        """
        return self.min_x, self.min_y

    def get_upper_right(self):
        """ Returns the upper right vertex of the bounding box

        :return: max_x, max_y
        :rtype: (float, float)
        """
        return self.max_x, self.max_y

    def get_middle(self):
        """ Returns the middle point of the bounding box

        :return: middle point
        :rtype: (float, float)
        """
        return (self.min_x + self.max_x) / 2, (self.min_y + self.max_y) / 2

    def get_crs(self):
        """ Returns the coordinate reference system (CRS) of the bounding box.

        :return: CRS that the BBox is given in
        :rtype: constants.CRS
        """
        return self.crs

    def __repr__(self):
        return "{}((({}, {}), ({}, {})), crs={})".format(self.__class__.__name__, self.min_x, self.min_y, self.max_x,
                                                         self.max_y, self.crs)

    def __str__(self):
        return "{},{},{},{}".format(self.min_x, self.min_y, self.max_x, self.max_y)

    @staticmethod
    def _to_tuple(bbox):
        """ Converts the input bbox representation (see the constructor docstring for a list of valid representations)
            into a flat tuple

        :param bbox: A bbox in one of 7 forms listed in the class description.
        :return: A flat tuple of size
        :raises: TypeError
        """
        if isinstance(bbox, (list, tuple)):
            return BBox._tuple_from_list_or_tuple(bbox)
        elif isinstance(bbox, str):
            return BBox._tuple_from_str(bbox)
        elif isinstance(bbox, dict):
            return BBox._tuple_from_dict(bbox)
        elif isinstance(bbox, BBox):
            return BBox._tuple_from_bbox(bbox)
        raise TypeError('Invalid bbox representation')

    @staticmethod
    def _tuple_from_list_or_tuple(bbox):
        """ Converts a list or tuple representation of a bbox into a flat tuple representation.

        :param bbox: a list or tuple with 4 coordinates that is either flat or nested
        :return: tuple (min_x,min_y,max_x,max_y)
        :raises: TypeError
        """
        if len(bbox) == 4:
            return tuple(map(float, bbox))
        elif len(bbox) == 2 and all([isinstance(point, (list, tuple)) for point in bbox]):
            return BBox._tuple_from_list_or_tuple(bbox[0] + bbox[1])
        raise TypeError('Expected a valid list or tuple representation of a bbox')


    @staticmethod
    def _tuple_from_str(bbox):
        """ Parses a string of the form 'min_x,min_y,max_x,max_y' into a flat tuple

        :param bbox: str of the form 'min_x,min_y,max_x,max_y'
        :return: tuple (min_x,min_y,max_x,max_y)
        """
        return tuple([float(s) for s in bbox.split(",")])

    @staticmethod
    def _tuple_from_dict(bbox):
        """ Converts a dictionary representation of a bbox into a flat tuple representation

        :param bbox: a dict with keys "min_x, "min_y", "max_x", and "max_y"
        :return: tuple (min_x,min_y,max_x,max_y)
        :raises: KeyError
        """
        return bbox['min_x'], bbox['min_y'], bbox['max_x'], bbox['max_y']

    @staticmethod
    def _tuple_from_bbox(bbox):
        """ Converts a BBox instance into a tuple

        :param bbox: An instance of the BBox type
        :return: tuple (min_x,min_y,max_x,max_y)
        """
        return bbox.get_lower_left() + bbox.get_upper_right()
