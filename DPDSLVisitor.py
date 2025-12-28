# Generated from DPDSL.g4 by ANTLR 4.13.1
from antlr4 import *
if "." in __name__:
    from .DPDSLParser import DPDSLParser
else:
    from DPDSLParser import DPDSLParser

# This class defines a complete generic visitor for a parse tree produced by DPDSLParser.

class DPDSLVisitor(ParseTreeVisitor):

    # Visit a parse tree produced by DPDSLParser#query.
    def visitQuery(self, ctx:DPDSLParser.QueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by DPDSLParser#select_clause.
    def visitSelect_clause(self, ctx:DPDSLParser.Select_clauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by DPDSLParser#select_item.
    def visitSelect_item(self, ctx:DPDSLParser.Select_itemContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by DPDSLParser#Aggregation.
    def visitAggregation(self, ctx:DPDSLParser.AggregationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by DPDSLParser#CountStar.
    def visitCountStar(self, ctx:DPDSLParser.CountStarContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by DPDSLParser#Parens.
    def visitParens(self, ctx:DPDSLParser.ParensContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by DPDSLParser#FloatLiteral.
    def visitFloatLiteral(self, ctx:DPDSLParser.FloatLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by DPDSLParser#Literal.
    def visitLiteral(self, ctx:DPDSLParser.LiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by DPDSLParser#LabeledColumn.
    def visitLabeledColumn(self, ctx:DPDSLParser.LabeledColumnContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by DPDSLParser#BinaryOp.
    def visitBinaryOp(self, ctx:DPDSLParser.BinaryOpContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by DPDSLParser#from_clause.
    def visitFrom_clause(self, ctx:DPDSLParser.From_clauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by DPDSLParser#where_clause.
    def visitWhere_clause(self, ctx:DPDSLParser.Where_clauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by DPDSLParser#group_by_clause.
    def visitGroup_by_clause(self, ctx:DPDSLParser.Group_by_clauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by DPDSLParser#groupByColumn.
    def visitGroupByColumn(self, ctx:DPDSLParser.GroupByColumnContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by DPDSLParser#label.
    def visitLabel(self, ctx:DPDSLParser.LabelContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by DPDSLParser#function_name.
    def visitFunction_name(self, ctx:DPDSLParser.Function_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by DPDSLParser#operator.
    def visitOperator(self, ctx:DPDSLParser.OperatorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by DPDSLParser#budget.
    def visitBudget(self, ctx:DPDSLParser.BudgetContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by DPDSLParser#identifier.
    def visitIdentifier(self, ctx:DPDSLParser.IdentifierContext):
        return self.visitChildren(ctx)



del DPDSLParser