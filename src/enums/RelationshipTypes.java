package enums;

import org.neo4j.graphdb.RelationshipType;

public enum RelationshipTypes implements RelationshipType

// IS_FRIEND_OF = DEEZERDB
// IS_CONNECTED = COOCCSDB
// ALL OTHER = MOVIEDB
{
	DIRECTED,PRODUCED,ACTED_IN,ACTED_WITH,HAS_KEYWORD,IN_GENRE,IS_FRIEND_OF,IS_CONNECTED;
}
