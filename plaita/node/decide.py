import json
from typing import Any, ClassVar, Dict, List, Optional, Union

from pydantic import BaseModel, Field, model_validator

from plaita.core import types
from ..io import Property, evaluate
from ..logger import logger
from .basic import Node

CONDITION_OP_EQ = "eq"
CONDITION_OP_NE = "ne"
CONDITION_OP_GT = "gt"
CONDITION_OP_GTE = "gte"
CONDITION_OP_LT = "lt"
CONDITION_OP_LTE = "lte"
CONDITION_OP_IN = "in"
CONDITION_OP_NOTIN = "notIn"
CONDITION_OP_CONTAINS = "contains"
CONDITION_OP_NOT_CONTAINS = "notContains"

condition_matcher = {
    CONDITION_OP_EQ: lambda left, right: left == right,
    CONDITION_OP_NE: lambda left, right: left != right,
    CONDITION_OP_GT: lambda left, right: left > right,
    CONDITION_OP_GTE: lambda left, right: left >= right,
    CONDITION_OP_LT: lambda left, right: left < right,
    CONDITION_OP_LTE: lambda left, right: left <= right,
    CONDITION_OP_IN: lambda left, right: left in right,
    CONDITION_OP_NOTIN: lambda left, right: left not in right,
    CONDITION_OP_CONTAINS: lambda left, right: right in left,
    CONDITION_OP_NOT_CONTAINS: lambda left, right: right not in left,
}

LOGIC_TYPE_AND = "and"
LOGIC_TYPE_OR = "or"


class Condition(BaseModel):
    field: Any
    operator: str
    value: Any

    def match(self, context, prefix="$"):
        left = evaluate(self.field, context, prefix)
        right = evaluate(self.value, context, prefix)

        # Handle None values
        if left is None or right is None:
            if self.operator == CONDITION_OP_EQ:
                return left is right
            elif self.operator == CONDITION_OP_NE:
                return left is not right
            return False

        return condition_matcher[self.operator](left, right)


class ConditionGroup(BaseModel):
    relation: str
    conditions: List[Union[Condition, "ConditionGroup"]] = Field(default_factory=list)

    def match(self, context, prefix=None):
        if not self.conditions:
            return True
        logic_func = all if self.relation == LOGIC_TYPE_AND else any
        return logic_func([condition.match(context, prefix) for condition in self.conditions])


class Branch(BaseModel):
    name: Optional[str] = None
    condition: Optional[Union[Condition, ConditionGroup]] = None
    priority: Optional[int] = 0
    label: Optional[str] = None
    desc: Optional[str] = None
    next: Optional[str] = None
    is_default: bool = Field(default=False)

    @model_validator(mode="before")
    @classmethod
    def setup_condition(cls, values: Dict) -> Dict:
        condition = values.get("condition")
        if condition and not isinstance(condition, (Condition, ConditionGroup)):
            values["condition"] = condition_from_json(condition)
        if "isDefault" in values:
            values["is_default"] = values.pop("isDefault")
        values["next"] = values.get("next", None)
        return values


def condition_from_json(content: Union[Condition, ConditionGroup, str, None]) -> Union[Condition, ConditionGroup, None]:
    if not content:
        return None

    if isinstance(content, (Condition, ConditionGroup)):
        return content

    if isinstance(content, str):
        content = json.loads(content)

    return _parse_condition_content(content)


def _parse_condition_content(content: dict) -> Union[Condition, ConditionGroup, None]:
    if "relation" in content and "conditions" in content:
        return _create_condition_group(content)
    elif "field" in content and "operator" in content and "value" in content:
        return _create_condition(content)
    return None


def _create_condition_group(content: dict) -> ConditionGroup:
    logic_type = content.get("relation")
    conditions = content.get("conditions", [])
    return ConditionGroup(
        relation=logic_type, conditions=[cond for cond in map(condition_from_json, conditions) if cond]
    )


def _create_condition(content: dict) -> Condition:
    return Condition(field=content.get("field"), operator=content.get("operator"), value=content.get("value"))


class Switch(Node):
    branching: ClassVar[bool] = True
    node_name: ClassVar[str] = "判断"
    node_type: ClassVar[str] = "switch"

    output_type: Property = Property(data_type=types.STRING, name="branch_name")

    branches: List[Branch] = Field(default_factory=list)

    @model_validator(mode="before")
    def setup_branches(cls, values: Dict) -> Dict:
        branches = values.get("branches", [])
        values["branches"] = []
        for branch in branches:
            if isinstance(branch, Branch):
                values["branches"].append(branch)
            elif isinstance(branch, dict):
                values["branches"].append(Branch(**branch))

        # 兼容一下output_type的命名，支持驼峰命名
        output_type = values.get("outputType") or values.get("output_type")
        if output_type:
            values["output_type"] = Property.model_validate(output_type)
        return values

    def validate(self):
        assert self.branches, "branches is required for decide node"

    def execute(self, execution):
        self.branches.sort(key=lambda x: x.priority, reverse=True)
        for branch in self.branches:
            if branch.condition and branch.condition.match(execution.context, prefix=execution.express_prefix):
                logger.info("test branches, %s, %s", branch.name, branch.next)
                return branch.next or branch.name
        for branch in self.branches:
            if branch.is_default:
                logger.info("default branches %s, %s", branch.name, branch.next)
                return branch.next or branch.name
        return None


class Bool(Switch):
    node_name: ClassVar[str] = "IF"
    node_type: ClassVar[str] = "if"

    else_next: Optional[str] = "false"
    next: Optional[str] = "true"
    condition: Optional[Union[Condition, ConditionGroup]] = None

    @model_validator(mode="before")
    @classmethod
    def setup_branches(cls, values: Dict) -> Dict:
        condition = values.get("condition")
        next_val = values.get("next", "true")
        else_next = values.get("else_next", "false")
        branches = [
            Branch(
                name=next_val,
                condition=condition,
                priority=0,
                label="true",
                desc="true",
                next=next_val,
                is_default=False,
            ),
            Branch(
                name=else_next, condition=None, priority=0, label="false", desc="false", next=else_next, is_default=True
            ),
        ]
        values["branches"] = branches
        return values


class SwitchLegacy(Switch):
    node_name: ClassVar[str] = "CASE"
    node_type: ClassVar[str] = "case"
    target: Any = None
    cases: List[Dict] = Field(default_factory=list)
    default: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def setup_branches(cls, values: Dict) -> Dict:
        values["target"] = values.get("target")
        cases = values.get("cases", [])
        default = values.get("default", "default")

        branches = [
            Branch(
                name=case["id"],
                condition=Condition(field=values.get("target"), operator=CONDITION_OP_EQ, value=case["value"]),
                priority=0,
                label=case["name"],
                next=case["id"],
                is_default=False,
            )
            for case in cases
        ]
        branches.append(
            Branch(name="default", condition=None, priority=0, label="default", next=default, is_default=True)
        )
        values["branches"] = branches
        return values


Logic = Switch
