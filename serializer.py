from abc import ABCMeta, abstractmethod

from lxml.etree import Element


class IXMLSerializer:
    __metaclass__ = ABCMeta

    @abstractmethod
    def from_xml(self, source_element: Element) -> 'IXMLSerializer': raise NotImplementedError
    @abstractmethod
    def to_xml(self) -> Element: raise NotImplementedError

