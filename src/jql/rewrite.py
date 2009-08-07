from jqlAST import *

class _ParseState(object):
    def __init__(self):
        self.labeledjoins = {}
        self.labeledjoinorder = []
        self.labelreferences = {}
        self._anonJoinCounter = 0

    def addLabeledJoin(self, name, join):
        if join.name:
            if join.name != name:
                raise QueryException(
                   "can't assign id %s, join already labeled %s: %s"
                    % (name, join.name, join))
        else:
            join.name = name

        self.labeledjoins.setdefault(name,[]).append(join)

        if name in self.labeledjoinorder:
            #outermost wins
            self.labeledjoinorder.remove(name)
        #assumes this is called in bottoms-up parse order
        self.labeledjoinorder.append(name)

    def getLabeledJoin(self, name):
        jlist = self.labeledjoins.get(name)
        if not jlist:
            return None
        return jlist[0]

    def nextAnonJoinId(self):
        self._anonJoinCounter += 1
        return '@' + str(self._anonJoinCounter)

    def joinMoved(self, join, from_, to):
        #print 'moving',join, 'from', from_, 'to', to
        #XXX test: { id = ?self and ?self = 1 }
        if not join.name:
            join.name = self.nextAnonJoinId()
        if not from_ or join is to or from_ is to:
            return False
        #XXX implement replaceArg for filter, any expression arg
        if from_.parent:
            from_.replaceArg(join, Label(join.name))
            return True
        return False

    def _joinFromConstruct(self, construct, where):
        '''
        build a join expression from the construct pattern
        '''
        left = where
        for prop in construct.args:
            if prop == construct.id:
                pass
            else:
                if isinstance(prop.value, Select):
                    value = prop.value.where
                else:
                    value = prop.value
                if value == Project('*'):
                    value = None
    
                if prop.nameIsFilter:
                    if value:
                        value = Eq(Project(prop.name), value)                    
                    else:
                        value = Project(prop.name)
                    prop.appendArg( Project(prop.name) )
                    
                    if not left:
                        left = value
                    else:
                        left = And(left, value)
                elif value:
                    #don't want treat construct values as boolean filter
                    #but we do want to find projections which we need to join
                    #(but we skip project(0) -- no reason to join)
                    for child in value.depthfirst(
                     descendPredicate=lambda op: not isinstance(op, ResourceSetOp)):
                        if isinstance(child, Project) and child.fields != [SUBJECT]:
                            import copy
                            if not left:                            
                                left = copy.copy( child )
                            else:
                                assert child
                                left = And(left, copy.copy( child ))
    
                #XXX: handle outer joins:
                #if prop.ifEmtpy == PropShape.omit:
                #    jointype = JoinConditionOp.RIGHTOUTER
                #else:
                #    jointype = JoinConditionOp.INNER
                #join.appendArg(JoinConditionOp(filter, SUBJECT,jointype))
    
        if left:
            left = self.makeJoinExpr(left)
            assert left
        
        if not left:
            left = Join()
    
        if construct.id:
            name = construct.id.getLabel()
            assert left
            try:
                self.addLabeledJoin(name, left)
            except:
                #print construct
                #print where
                #print left
                raise
    
        return left

    def _buildJoinsFromReferences(self, labeledjoins):
        skipped = []
        for join, conditions in self.labelreferences.items():
            currentjoin = join
    
            def labelkey(item):
                label = item[0]
                try:
                    return self.labeledjoinorder.index(label)
                except ValueError:
                    return 999999 #sort at end
            #sort by order of labeled join appearence
            #XXX is that enough for correctness? what about sibling joins?
            conditions.sort(key=labelkey)        
            for label, (op, pred) in conditions:
                labeledjoin = labeledjoins.get(label)
                if not labeledjoin:
                    if label in skipped:
                        #XXX support unlabeled joins
                        raise QueryException('unlabeled joins not yet supported')
                    else:
                        #XXX keep skipped around to check if there are construct labels
                        #for this label, if not, emit warning
                        skipped.append(label)
                    continue
    
                if op is join:
                    #any subsequent join predicates should operate on the new join
                    op = currentjoin
                if op is not labeledjoin:
                    if isinstance(op, Join):
                        self.joinMoved(op, op.parent, labeledjoin)
                    labeledjoin.appendArg(JoinConditionOp(op, pred))
                currentjoin = labeledjoin
    
        if skipped: #XXX should just be warning?
            raise QueryException(
                    'reference to unknown label(s): '+ ', '.join(skipped))
        return skipped

    logicalops = {
     And : Join,
     Or : Union,
    }
    
    def _getASTForProject(self, project):
        '''
        Return an op that will retrieve the values that match the projection
    
        bar return 
        
        Filter(Eq('bar', Project(PROPERTY)), objectlabel='bar'))
    
        foo.bar returns
    
        jc(Join(
            jc(Filter(Eq('bar', Project(PROPERTY)), objectlabel='bar'), OBJECT),
            jc(Filter(Eq('foo', Project(PROPERTY)) ), SUBJECT)
          ),
        '_1')
    
        JoinCondition(
        Join(
            jc(Filter(None, Eq('bar'), None, subjectlabel='_1'), OBJECT),
            jc(Filter(None, Eq('baz'), None, objectlabel='baz'), OBJECT)
          )
        '_1')
    
        In other words, join the object value of the "bar" filter with object value
        of "baz" filter.
        We add the label'baz' so that the project op can retrieve that value.
        The join condition join this join back into the enclosing join
        using the subject of the "bar" filter.
    
        bar == { ?id where }
    
        Filter(Eq('bar'), Join(jc(
            Filter(None, Eq('foo'), None, propertyname='foo'), 'foo'))
    
        ?foo.bar is shorthand for
        { id : ?foo where(bar) }
        thus:
        Join( Filter(Eq('bar',Project(PROPERTY)), subjectlabel='foo', objectlabel='bar') )
    
        '''
        #XXX we need to disabiguate labels with the same name
        op = None
        if project.name == SUBJECT:
            assert not project.varref
            return op
    
        for propname in reversed(project.fields):
            #XXX if propname == '*', * == OBJECT? what about foo = * really a no-op
            if not op:
                op = Filter(Eq(propname, Project(PROPERTY)), objectlabel=propname)
            else:
                subjectlabel = self.nextAnonJoinId()
                filter = Filter(Eq(propname, Project(PROPERTY)),
                                                subjectlabel=subjectlabel)
                #create a new join, joining the object of this filter with
                #the subject of the prior one
                op = JoinConditionOp(
                        Join( JoinConditionOp(op, SUBJECT),
                            JoinConditionOp(filter, OBJECT)), subjectlabel)
    
        if project.varref:
            #XXX fix this questionable hack: addLabel should be available on Joins, etc.
            for child in op.breadthfirst():
                if isinstance(child, Filter):
                    child.addLabel(project.varref, SUBJECT)
                    break
            op = Join(op)
            self.addLabeledJoin(project.varref, op)
        
        return op
        
    def makeJoinExpr(self, expr):
        '''
        Rewrite expression into Filters, operations that filter rows
        and ResourceSetOps (join, union, except), which group together the Filter 
        results by id (primary key).
        
        We also need to make sure that filter which apply individual statements
        (id, property, value) triples appear before filters that apply to more than
        one statement and so operate on the simple filter results.
    
        filters to test:
        foo = (?a or ?b)
        foo = (a or b)
        foo = (?a and ?b)
        foo = (a and b)
        foo = {c='c'}
        foo = ({c='c'} and ?a)
        '''
        cmproots = []
        to_visit = []
        visited = set()
        to_visit.append( (None, expr) )
    
        labeledjoins = self.labeledjoins
        labelreferences = self.labelreferences
    
        newexpr = None
        while to_visit:
            parent, v = to_visit.pop()
            if id(v) not in visited:
                visited.add( id(v) )
    
                notcount = 0
                while isinstance(v, Not):
                    notcount += 1
                    assert len(v.args) == 1
                    v = v.args[0]
                
                optype = self.logicalops.get(type(v))
                if optype:
                    if notcount % 2: #odd # of nots
                    #if the child of the Not is a logical op, we need to treat this
                    #as a Except op otherwise just include it in the compare operation
                        notOp = Except()
                        if not parent:
                            parent = newexpr = notOp
                        else:
                            parent.appendArg(notOp)
                            parent = notOp
    
                    if not parent:
                        parent = newexpr = optype()
                    elif type(parent) != optype:
                        #skip patterns like: and(and()) or(or())
                        newop = optype()
                        parent.appendArg(newop)
                        parent = newop
                    
                    to_visit.extend([(parent, a) for a in v.args]) #descend
                else: #if not isinstance(v, Join): #joins has already been processed
                    if not parent: #default to Join
                        parent = newexpr = Join()
                    if notcount % 2:
                        v = Not(v)
                    cmproots.append( (parent, v) )
    
        #for each top-level comparison in the expression    
        for parent, root in cmproots:
            #first add filter or join conditions that correspond to the columnrefs
            #(projections) that appear in the expression
    
            #look for Project ops but don't descend into ResourceSetOp (Join) ops
            projectops = []
            skipRoot = False
            labels ={}
            
            for child in root.depthfirst(
                    descendPredicate=lambda op: not isinstance(op, ResourceSetOp)):
                if isinstance(child, ResourceSetOp):                
                    if child is root or (child.parent is root
                                        and isinstance(child.parent, Not)):
                        #XXX standalone join, do an "(not) exists join"
                        skipRoot = True #don't include this root in this join
                    else:
                        if not child.name:
                            child.name = self.nextAnonJoinId()
                            self.addLabeledJoin(child.name, child)
                        child = Label(child.name)
                        #replace this join with a Label
                        #XXX same as Label case (assign label if necessary)
                        raise QueryException('join in filter not yet implemented: %s' % root)
                if isinstance(child, Project):
                     projectop = self._getASTForProject(child)
                     if projectop:
                        projectops.append( (child, projectop) )
                elif isinstance(child, Label):
                    labels.setdefault(child.name,[]).append(child)
    
            if len(labels) > 1:
                if len(labels) == 2:
                    ##XXX buildJoinsFromReferences asserts: pos 0 but not a Filter: <class Label>
                    #with {id=?a and ?a = 1} and {id=?b and ?b = 2} and ?b = ?a
                    #XXX currently only handle patterns like ?a = ?b
                    #need to handle pattern like "foo = (?a or ?b)" (boolean)
                    # or "?a = ?b = ?c"  or foo(?a,?b) or ?a != ?b
                    (a, b) = [v[0] for v in labels.values()]
                    #XXX fix this hack hardcoding recurse func
                    if root == Eq(a, b) or root == Eq(recurse(a), b) or root == Eq(recurse(b), a):
                        parentjoin = self.getLabeledJoin(a.name)                        
                        childjoin = self.getLabeledJoin(b.name)                                                
                        if parentjoin and childjoin:
                            joincond = (childjoin, b.name) #(op to join with, join pred)
                            labelreferences.setdefault(parentjoin, []).append(
                                                (b.name, joincond) )
                        else:
                            #XXX handle the case where joins that the labels are 
                            #associated with have not been encountered yet  
                            raise QueryException(
                        'could not find reference to labels %s or %s' % (a.name, b.name))
                        skipRoot = True
                else:
                    raise QueryException('expressions like ?a = ?b not yet supported')
            else:
                #handle filter conditions that contain a label (which is a reference to another join)
                for labelname, ops in labels.items():
                    child = ops[0] #XXX need to worry about expressions like foo(?a, ?a) ?
                    if root == Eq(Project(SUBJECT), child):
                        #its a declaration like id = ?label
                        self.addLabeledJoin(labelname, parent)
                    else: #label reference
                        child.__class__ = Constant #hack so label is treated as independant
                        if root.isIndependent():
                            #the filter only depends on the label's join, not the parent join
                            #so we can just treat as it as a filter on the parent
                            joincond = (Filter(root), SUBJECT) #filter, join pred
                        else:
                            #depends on both the parent and the label's join, 
                            #so join them together
                            joincond = (parent, root) #join, join pred
                        #replace the label reference with a Project(SUBJECT):
                        Project(SUBJECT)._mutateOpToThis(child)
                        labelreferences.setdefault(parent, []).append(
                                                            (labelname, joincond) )
                        
                    skipRoot = True #don't include this root in this join
    
            #try to consolidate the projection filters into root filter.
            if skipRoot:
                filter = Filter()
            else:
                filter = Filter(root)
                consolidateFilter(filter, projectops)
            for (project, projectop) in projectops:
                parent.appendArg(projectop)
            if not skipRoot:
                parent.appendArg( filter )
    
        #XXX remove no-op and redundant filters
        assert newexpr
        return newexpr

def consolidateFilter(filter, projections):
    '''
    Crucial optimization is to consolidate filters that can be applied at once:
    
    e.g. instead foo = 'bar' being:


    Filter(Eq(Project(PROPERTY), 'foo'), objectlabel='foo')
    Filter(Eq(Project('foo'),'bar'))

    consolidate those into:

    Filter(Eq(Project(PROPERTY), 'foo'),Eq(Project(OBJECT), 'bar'), objectlabel='foo')

    A consolidated filter can only have predicate per position

    e.g. foo = bar

    goes from:

    Filter(Eq(Project(PROPERTY), 'foo'), objectlabel='foo')
    Filter(Eq(Project(PROPERTY), 'bar'), objectlabel='bar')
    Filter(Eq(Project('foo'),Project('bar')))

    only can be:

    Filter(Eq(Project(PROPERTY), 'bar'), objectlabel='bar')
    Filter(Eq(Project(PROPERTY), 'foo'),Eq(Project(OBJECT), Project('bar')) )

    however, this isn't much of an optimization, so, for simplicity, we don't bother
    '''
    #XXX consolidate subject filters
    filterprojects = [(p, i) for (i, (p,f)) in enumerate(projections)
                                                if isinstance(f, Filter)]
    if len(filterprojects) == 1:
        p, i = filterprojects[0]
        name = p.name
        assert len(p.fields) == 1
        p.fields = [OBJECT] #replace label with pos
        filter.appendArg( Eq(Project(PROPERTY), name) )
        filter.addLabel(name, OBJECT)
        #remove replaced filter:
        del projections[i]
        return True
    return False


def rewriteLabelsInFilters():
    '''
    Filter conditions that depend are a label are actually join predicates on
    the object that the label references. So we need to rewrite the filter and
    add join conditions:

    * build a multimap between joins and labels (label => [join])
    whenever encountered: id = ?label, ?foo.bar and id : ?foo
    * build joincondition map for other references to labels
      (label => [(join, joinconditionpred)]), exclude joinconditionpred from filter
    * when done parsing, join together joins that share labels,
      removing any joined joins if their parent is a dependant (non-root) Select op

    this:
    {
    id : ?owner,
    'mypets' : {
          'dogs' : { * where(owner=?owner and type='dog') },
          'cats' : { * where(owner=?owner and type='cat') }
        }
    }

    equivalent to:
    {
    id : ?owner,

    'mypets' : {
          'dogs' : { * where(id = ?pet and type='dog') },
          'cats' : { * where(id = ?pet and type='cat') }
        }

    where ( {id = ?pet and owner=?owner} )
    }

    (but what about where ( { not id = ?foo or id = ?bar and id = ?baz }

    also, this:
    {
    'foo' : ?baz.foo
    'bar' : ?baz.bar
    }
    results in joining ?baz together

    here we don't join but use the label to select a value
    { 
      'guardian' : ?guardian,
      'pets' : { * where(owner=?guardian) },
    }

this is similar but does trigger a join on an unlabeled object:
    {
      'guardian' : ?guardian,
      'dogs' : { * where(owner=?guardian and type='dog') },
      'cats' : { * where(owner=?guardian and type='cat') }
    }

join( filter(eq(project('type'), 'dog')),
     filter(eq(project('owner'),objectlabel='guardian')
  jc(
     join( filter(eq(project('type'), 'cat')),
          filter(eq(project('owner'),objectlabel='guardian')
     ),
     Eq(Project('guardian'), Project('guardian'))
)

XXX test multiple labels in one filter, e.g.: { a : ?foo, b : ?bar where (?foo = ?bar) }
XXX test self-joins e.g. this nonsensical example: { * where(?foo = 'a' and ?foo = 'b') }
    '''
