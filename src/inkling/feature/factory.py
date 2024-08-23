from inkling.feature.feedback_yn_ratio import PositiveRatio
from inkling.feature.item_intro_length import ItemIntroLength
from inkling.feature.item_badge_types import ItemBadgeTypes


class FeatureNotFoundException(Exception):
    def __init__(self, feature_name):
        self.feature_name = feature_name


PERIODIC_FEATURE_CLASSES = [
]

GENERAL_FEATURE_CLASSES = [
    PositiveRatio,
    ItemIntroLength,
    ItemBadgeTypes,
]


class PeriodicFeatureFactory:

    @classmethod
    def create(cls, feature_name, service_config, domain_path, phase_type):
        for feature_class in PERIODIC_FEATURE_CLASSES:
            if feature_class.schema_type == feature_name:
                return feature_class(service_config, domain_path, phase_type)

        raise FeatureNotFoundException(feature_name)


class GeneralFeatureFactory:

    @classmethod
    def create(cls, feature_name, service_config, domain_path, phase_type):
        for feature_class in GENERAL_FEATURE_CLASSES:
            if feature_class.schema_type == feature_name:
                return feature_class(service_config, domain_path, phase_type)

        raise FeatureNotFoundException(feature_name)
