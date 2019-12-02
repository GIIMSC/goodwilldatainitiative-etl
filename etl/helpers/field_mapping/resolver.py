from etl.helpers.field_mapping.common import FieldMapping, FieldMappings, APPROVED


class FieldMappingResolver:
    @staticmethod
    def _remove_unapproved_mappings(mapping: FieldMapping):
        """
        Removes unnaproved mappings from a FieldMapping.
        """
        mapping_dict = mapping.get_field_mapping_dict()

        approved_dict = {
            input: (output, approved)
            for input, (output, approved) in mapping_dict.items()
            if approved == APPROVED
        }

        return FieldMapping.from_dict(approved_dict)

    @staticmethod
    def _resolve_mapping(
        new_mapping: FieldMapping,
        source_mapping: FieldMapping,
        overwrite: bool = False,
        remove_unapproved_source_mappings: bool = True,
    ) -> FieldMapping:
        """
        Combines source and new field mappings. May overwrite source mappings
        or remove unapproved source mappings.
        """
        if remove_unapproved_source_mappings:
            source_mapping = FieldMappingResolver._remove_unapproved_mappings(
                source_mapping
            )
        source_mapping_dict = source_mapping.get_field_mapping_dict()
        new_mapping_dict = new_mapping.get_field_mapping_dict()

        # If we want to overwrite mappings that are in the source mapping, put the new mapping second
        # If we don't want to overwrite mappings that are in the source mapping, put the new mapping first
        resolved_mapping_dict = (
            {**source_mapping_dict, **new_mapping_dict}
            if overwrite
            else {**new_mapping_dict, **source_mapping_dict}
        )
        return FieldMapping.from_dict(resolved_mapping_dict)

    @staticmethod
    def resolve_mappings(
        new_mappings: FieldMappings,
        source_mappings: FieldMappings,
        overwrite: bool = False,
        remove_unapproved_source_mappings: bool = True,
    ) -> FieldMappings:
        """
        Resolves a set of source and new FieldMappings.
        """
        resolved_mappings = {}

        source_fields = set(source_mappings.keys())
        new_fields = set(new_mappings.keys())

        for field in source_fields | new_fields:
            if field in source_fields:
                source_mapping = source_mappings[field]
            else:
                source_mapping = FieldMapping.from_dict({})

            if field in new_fields:
                new_mapping = new_mappings[field]
            else:
                new_mapping = FieldMapping.from_dict({})

            resolved_mapping = FieldMappingResolver._resolve_mapping(
                new_mapping,
                source_mapping,
                overwrite,
                remove_unapproved_source_mappings,
            )

            if not resolved_mapping.is_empty():
                resolved_mappings[field] = resolved_mapping

        return resolved_mappings
