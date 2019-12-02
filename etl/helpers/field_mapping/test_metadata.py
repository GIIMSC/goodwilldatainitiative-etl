from tableschema import Schema

TEST_SCHEMA: Schema = Schema(
    {
        "fields": [
            {
                "name": "field1",
                "type": "integer",
                "constraints": {"minimum": 1, "maximum": 2},
                "enum_mapping": {
                    "Hispanic/Latino ethnic origin": 1,
                    "Not Hispanic/Latino": 2,
                },
            },
            {
                "name": "field2",
                "type": "integer",
                "allows_multiple": True,
                "constraints": {"minimum": 1, "maximum": 10},
                "enum_mapping": {
                    "Blindness or Other Visual Impairment": 1,
                    "Deafness or Hard of Hearing": 2,
                    "Other Physical Disability": 3,
                    "Neurological Disability": 4,
                    "Learning Disability other than Autism": 5,
                    "Developmental Disability other than Autism": 6,
                    "Autism": 7,
                    "Psychiatric Disability": 8,
                    "Emotional Disability": 9,
                    "Other disabling condition": 10,
                },
            },
            {
                "name": "field3",
                "type": "boolean",
                "trueValues": ["yes", "Y", "true", "T", "1"],
                "falseValues": ["no", "N", "false", "F", "2"],
            },
        ]
    }
)
