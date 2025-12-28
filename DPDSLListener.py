# Generated from DPDSL.g4 by ANTLR 4.13.1
from antlr4 import *
if "." in __name__:
    from .DPDSLParser import DPDSLParser
else:
    from DPDSLParser import DPDSLParser

# This class defines a complete listener for a parse tree produced by DPDSLParser.
class DPDSLListener(ParseTreeListener):

    # Enter a parse tree produced by DPDSLParser#query.
    def enterQuery(self, ctx:DPDSLParser.QueryContext):
        pass

    # Exit a parse tree produced by DPDSLParser#query.
    def exitQuery(self, ctx:DPDSLParser.QueryContext):
        pass


    # Enter a parse tree produced by DPDSLParser#select_clause.
    def enterSelect_clause(self, ctx:DPDSLParser.Select_clauseContext):
        pass

    # Exit a parse tree produced by DPDSLParser#select_clause.
    def exitSelect_clause(self, ctx:DPDSLParser.Select_clauseContext):
        pass


    # Enter a parse tree produced by DPDSLParser#select_item.
    def enterSelect_item(self, ctx:DPDSLParser.Select_itemContext):
        pass

    # Exit a parse tree produced by DPDSLParser#select_item.
    def exitSelect_item(self, ctx:DPDSLParser.Select_itemContext):
        pass


    # Enter a parse tree produced by DPDSLParser#Aggregation.
    def enterAggregation(self, ctx:DPDSLParser.AggregationContext):
        pass

    # Exit a parse tree produced by DPDSLParser#Aggregation.
    def exitAggregation(self, ctx:DPDSLParser.AggregationContext):
        pass


    # Enter a parse tree produced by DPDSLParser#CountStar.
    def enterCountStar(self, ctx:DPDSLParser.CountStarContext):
        pass

    # Exit a parse tree produced by DPDSLParser#CountStar.
    def exitCountStar(self, ctx:DPDSLParser.CountStarContext):
        pass


    # Enter a parse tree produced by DPDSLParser#Parens.
    def enterParens(self, ctx:DPDSLParser.ParensContext):
        pass

    # Exit a parse tree produced by DPDSLParser#Parens.
    def exitParens(self, ctx:DPDSLParser.ParensContext):
        pass


    # Enter a parse tree produced by DPDSLParser#FloatLiteral.
    def enterFloatLiteral(self, ctx:DPDSLParser.FloatLiteralContext):
        pass

    # Exit a parse tree produced by DPDSLParser#FloatLiteral.
    def exitFloatLiteral(self, ctx:DPDSLParser.FloatLiteralContext):
        pass


    # Enter a parse tree produced by DPDSLParser#Literal.
    def enterLiteral(self, ctx:DPDSLParser.LiteralContext):
        pass

    # Exit a parse tree produced by DPDSLParser#Literal.
    def exitLiteral(self, ctx:DPDSLParser.LiteralContext):
        pass


    # Enter a parse tree produced by DPDSLParser#LabeledColumn.
    def enterLabeledColumn(self, ctx:DPDSLParser.LabeledColumnContext):
        pass

    # Exit a parse tree produced by DPDSLParser#LabeledColumn.
    def exitLabeledColumn(self, ctx:DPDSLParser.LabeledColumnContext):
        pass


    # Enter a parse tree produced by DPDSLParser#BinaryOp.
    def enterBinaryOp(self, ctx:DPDSLParser.BinaryOpContext):
        pass

    # Exit a parse tree produced by DPDSLParser#BinaryOp.
    def exitBinaryOp(self, ctx:DPDSLParser.BinaryOpContext):
        pass


    # Enter a parse tree produced by DPDSLParser#from_clause.
    def enterFrom_clause(self, ctx:DPDSLParser.From_clauseContext):
        pass

    # Exit a parse tree produced by DPDSLParser#from_clause.
    def exitFrom_clause(self, ctx:DPDSLParser.From_clauseContext):
        pass


    # Enter a parse tree produced by DPDSLParser#where_clause.
    def enterWhere_clause(self, ctx:DPDSLParser.Where_clauseContext):
        pass

    # Exit a parse tree produced by DPDSLParser#where_clause.
    def exitWhere_clause(self, ctx:DPDSLParser.Where_clauseContext):
        pass


    # Enter a parse tree produced by DPDSLParser#group_by_clause.
    def enterGroup_by_clause(self, ctx:DPDSLParser.Group_by_clauseContext):
        pass

    # Exit a parse tree produced by DPDSLParser#group_by_clause.
    def exitGroup_by_clause(self, ctx:DPDSLParser.Group_by_clauseContext):
        pass


    # Enter a parse tree produced by DPDSLParser#groupByColumn.
    def enterGroupByColumn(self, ctx:DPDSLParser.GroupByColumnContext):
        pass

    # Exit a parse tree produced by DPDSLParser#groupByColumn.
    def exitGroupByColumn(self, ctx:DPDSLParser.GroupByColumnContext):
        pass


    # Enter a parse tree produced by DPDSLParser#label.
    def enterLabel(self, ctx:DPDSLParser.LabelContext):
        pass

    # Exit a parse tree produced by DPDSLParser#label.
    def exitLabel(self, ctx:DPDSLParser.LabelContext):
        pass


    # Enter a parse tree produced by DPDSLParser#function_name.
    def enterFunction_name(self, ctx:DPDSLParser.Function_nameContext):
        pass

    # Exit a parse tree produced by DPDSLParser#function_name.
    def exitFunction_name(self, ctx:DPDSLParser.Function_nameContext):
        pass


    # Enter a parse tree produced by DPDSLParser#operator.
    def enterOperator(self, ctx:DPDSLParser.OperatorContext):
        pass

    # Exit a parse tree produced by DPDSLParser#operator.
    def exitOperator(self, ctx:DPDSLParser.OperatorContext):
        pass


    # Enter a parse tree produced by DPDSLParser#budget.
    def enterBudget(self, ctx:DPDSLParser.BudgetContext):
        pass

    # Exit a parse tree produced by DPDSLParser#budget.
    def exitBudget(self, ctx:DPDSLParser.BudgetContext):
        pass


    # Enter a parse tree produced by DPDSLParser#identifier.
    def enterIdentifier(self, ctx:DPDSLParser.IdentifierContext):
        pass

    # Exit a parse tree produced by DPDSLParser#identifier.
    def exitIdentifier(self, ctx:DPDSLParser.IdentifierContext):
        pass



del DPDSLParser