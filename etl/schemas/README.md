# Schema Format

All schemas use the [Frictionless Data Table Schema](https://frictionlessdata.io/specs/table-schema/) format, with some modifications described below.


## Column-based Milestone Names

The are slight differences between the milestone names used in column-based field names and the accepted values for MilestoneFlag values.

* The MilestoneFlag value for midpoint is `Midpoint`, but the column-based field names use `MidPoint`, as in `MidPointWorkReadinessScore`
* The MilestoneFlag value for 2 year follow-ups is plural (`TwoYears`), but the column-based field name prefixes are not plural (`TwoYear`), as in `TwoYearWorkReadinessScore`
  * The same goes for `ThreeYears`, `FourYears`, and `FiveYears`

As a result, we include a key-value pair `column_based_milestone_names` in the schema, which lists the milestone names as they are used in the column-based field names. This is used when converting from column-based to row-based format.

## Custom Keys

There are several custom keys added for defining fields:

* `allows_multiple`
  * Boolean that indicates that this field accepts multiple values. Multiple value fields are parsed using a multiple value delimiter provided by the local Goodwill, and are processed in the pipeline as arrays
  * Example value: `true`
* `enum_mapping`
  * Maps text value to numerical value for enum fields
  * Fields with this key should be integer type
  * Example value: `{"Not at all confident": 0, "Somewhat confident": 1,  "Very confident": 2 }`
* `milestones`
  * Indicates the milestones that a field is collected for
  * If no `milestones` key is provided, then the field is an administrative field that can be collected at any milestone AND has the same name in column-based format regardless of milestone
  * Example value: `[0, 2, 6, 7]`
* `custom_milestone_field_names`
  * To derive the column-based name/key from the row-based name, the default is to add the milestone flag as a prefix (i.e. `IntakeDate`, `ExitDate`, `NinetyDaysDate`). However, some fields do not use this naming convention (i.e. `IntakeCurrentMostRecentJobLastDayOfWork` and `ExitJobPlacementStartDate` for `JobDate`). For fields that differ from the default naming convention, the names are provided in this field as a map of `milestone number -> custom field name`
  * Example value: `{"2": "ExitJobPlacementSOC"}`



# Making spec changes

## Table of Contents

* [Mission Impact Spec](#mission-impact-spec)
    * [Adding a new field](#adding-a-new-field)
        * [If the field is optional](#if-the-field-is-optional)
        * [If the field is required](#if-the-field-is-required)
    * [Removing an existing field](#removing-an-existing-field)
        * [If the field is optional](#if-the-field-is-optional-1)
        * [If the field is required](#if-the-field-is-required-1)
    * [Changing whether a field is required](#changing-whether-a-field-is-required)
        * [Required → Optional](#required--optional)
        * [Optional → Required](#optional--required)
    * [Changing the values accepted for a field](#changing-the-values-accepted-for-a-field)
        * [Adding a new value](#adding-a-new-value)
        * [Removing an existing value](#removing-an-existing-value)
        * [Changing the text for an existing value](#changing-the-text-for-an-existing-value)
    * [Changing a field's type](#changing-a-fields-type)
        * [Integer → Decimal](#integer--decimal)
        * [Boolean → Enum](#boolean--enum)
    * [Changing which milestones a field is collected at](#changing-which-milestones-a-field-is-collected-at)
        * [If adding a milestone](#if-adding-a-milestone)
        * [If removing a milestone](#if-removing-a-milestone)

* [Programs spec](#programs-spec)

## Mission Impact Spec

### Adding a new field

#### If the field is optional
1. Add that field to the published spec, all databases, all mission_impact tables in Wherescape (at GII), and all transformation scripts within Gateway
1. Add the field to [mission_impact_table_schema.json](mission_impact_table_schema.json)
1. Add the field to the Google Forms Solution
1. Sites can start collecting the new field
    * If a site names their column something that does not match the spec exactly, they'll receive an email telling them to update their column mappings

#### If the field is required
1. Follow steps for [If the field is optional](#if-the-field-is-optional)
1. Follow steps for [changing status from Optional -> Required](#optional--required)

### Removing an existing field

#### If the field is required
1. Follow steps for [changing status from Required -> Optional](#required--optional)
1. Follow steps for [removing an Optional field](#if-the-field-is-optional-1)

#### If the field is optional
1. Remove the field from the published spec, and alert sites that they can stop collecting/sending that field
1. Remove the field from the Google Forms Solution
1. Remove the field from [mission_impact_table_schema.json](mission_impact_table_schema.json)
    * If a site is still collecting the field or still has it in their database, they'll receive an email telling them to update their column mappings
1. Make any changes required in the GII databases / Gateway to stop accepting the field and clean up the data

### Changing whether a field is required

#### Required → Optional
1. Confirm that no checks exist within Gateway that the field must exist
1. Update the published spec to no longer say that the field is required
1. Update the Google Forms Solution
1. Remove `"required": true` from the field's definition in [mission_impact_table_schema.json](mission_impact_table_schema.json)

#### Optional → Required
1. Update the published spec + the Google Forms solution, and give sites some time to start sending the field
    * If a site was not sending it before, they may receive an email telling them to update their column mappings
1. Once all sites are sending it (or after a reasonable amount of time has passed), add `"required": true` to the constraints section of the field's definition in [mission_impact_table_schema.json](mission_impact_table_schema.json). Use CaseNumber as an example to see how this is done.
1. Update validation checks within Gateway.

### Changing the values accepted for a field

#### Adding a new value

_Example: Adding a new milestone, adding a new education value, etc._

1. Update any checks within Gateway for what values the field can take
1. Update the field's constraints in [mission_impact_table_schema.json](mission_impact_table_schema.json)
    * If the field takes a value from a list of possible text values (such as MilestoneFlag, which can be Intake, Midpoint, etc.) add it to the "enum" list
    * If the field takes a value from a list of numerical values (such as Gender, where Female is 1, Male is 2, etc.) add it and the expected number to the "enum_mapping" list
1. Update the published spec to mention the new value
1. Update the Google Forms Solution to include the new value

#### Removing an existing value

1. Update the published spec to no longer include the value that was removed, and give sites some time to make sure they aren't sending that value
1. Remove the value from the Google Forms Solution
1. Remove the value from the relevant "enum" or "enum_mapping" list in [mission_impact_table_schema.json](mission_impact_table_schema.json)
1. If Gateway does any checks on what values the field can take, update those checks.

#### Changing the text for an existing value

_Example: changing the wording for one of the "score" questions_

1. Update any checks within Gateway to allow the field to take both the old value and the new value
1. Update the field's enum list or enum mapping in [mission_impact_table_schema.json](mission_impact_table_schema.json) to include the new value, but do not remove the old value yet.
1. Update the field's text in the Google Forms Solution
1. Update the published spec to change the value from the old value to the new value, and give sites some time to make sure they aren't sending that value
1. Remove the old value from the enum list or enum mapping in the table schema
1. If Gateway does any checks on what values the field can take, update those checks.

### Changing a field's type

This section covers changing a field's type from something more restrictive to something less restrictive. For example, integer → decimal, or boolean → enum.

#### Integer → Decimal

1. Update any checks within Gateway to allow a decimal value
1. Update the field's type in [mission_impact_table_schema.json](mission_impact_table_schema.json) from "integer" to "number"
1. Update restrictions in the Google Forms Solution
1. Update the published spec to reference the new type

#### Boolean → Enum

1. Update the field's type in [mission_impact_table_schema.json](mission_impact_table_schema.json) from "boolean" to "integer", and add an "enum_mapping" for all previous accepted boolean values (true, false, yes, no, etc.) to the numeric value that it represents
1. Update any checks within Gateway to accept the enum values
1. Add the new enum values to the enum_mapping in [mission_impact_table_schema.json](mission_impact_table_schema.json)
1. Update the Google Forms Solution
1. Update the published spec to reference the new values

TODO: one value --> multiple values

### Changing which milestones a field is collected at

#### If adding a milestone
1. Modify the published spec, all databases, all mission_impact tables in Wherescape (at GII), and all transformation scripts within Gateway to ensure that the field can be collected for the additional milestone
1. Add the milestone index to the `milestone` list for the field in [mission_impact_table_schema.json](mission_impact_table_schema.json)
  * If the new column-based name do not use the standard naming convention, update `custom_milestone_field_names` for that field as well
1. Edit Google Forms Solution to make sure that this field is collected in the new milestone
1. Sites can start collecting the field at the new milestone

#### If removing a milestone
1. Update the published spec, and alert sites that they can stop collecting/sending the field for the removed milestone
  * For sites that send data in row-based form, continuing to send the field for the removed milestone will not cause any issues
  * For sites that send data in column-based form, continuing to send the field for that milestone will cause the data shape to be invalid - a quick fix is to create a column mapping that maps the column name for the removed milestone to an empty value
1. Edit Google Forms Solution to make sure that this field is not collected for the removed milestone
1. Remove the milestone index from the `milestone` list for the field in [mission_impact_table_schema.json](mission_impact_table_schema.json)
1. Make any changes required in the GII databases / Gateway to stop accepting the field for that milestone and clean up the data
