package org.apache.carbondata.presto;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.carbondata.core.datastore.impl.FileFactory;

import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestCarbondataConnectorFactory {

  private final String connectorName = "connector";
  ClassLoader classLoader;
  CarbondataConnectorFactory carbondataConnectorFactory =
      new CarbondataConnectorFactory(connectorName, getClassLoader());

  private static ClassLoader getClassLoader() {
    return FileFactory.class.getClassLoader();
  }

  @Test public void getName() {
    assertEquals("connector", carbondataConnectorFactory.getName());
  }

  @Test public void getHandleResolver() {
    assertTrue(carbondataConnectorFactory.getHandleResolver() instanceof CarbondataHandleResolver);
  }

  @Test public void create() {
    CarbondataConnector carbondataConnector;
    Map<String, String> m = new HashMap<String, String>();
    m.put("firstvalue", "one");
    m.put("secondvalue", "two");

   /* new MockUp<ConnectorContext>() {
       @Mock
     public TypeManager getTypeManager() {
        System.out.println("getTypeManager mocked!!");
        return new TypeManager() {

        };
      }
    };*/
    ConnectorContext connectorContext = new ConnectorContext() {
      @Override public NodeManager getNodeManager() {
        return null;
      }

      @Override public TypeManager getTypeManager() {
        return new TypeManager() {
          @Override public Type getType(TypeSignature typeSignature) {
            return null;
          }

          @Override public Type getParameterizedType(String s, List<TypeSignatureParameter> list) {
            return null;
          }

          @Override public List<Type> getTypes() {
            return null;
          }

          @Override public Optional<Type> getCommonSuperType(Type type, Type type1) {
            return null;
          }

          @Override public boolean isTypeOnlyCoercion(Type type, Type type1) {
            return false;
          }

          @Override public Optional<Type> coerceTypeBase(Type type, String s) {
            return null;
          }
        };
      }
    };
    carbondataConnectorFactory.create("connectorid", m, connectorContext);
  }

}
