/**
 *    Copyright 2009-2019 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.ibatis.builder.xml;

import java.io.InputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.ibatis.builder.BaseBuilder;
import org.apache.ibatis.builder.BuilderException;
import org.apache.ibatis.builder.CacheRefResolver;
import org.apache.ibatis.builder.IncompleteElementException;
import org.apache.ibatis.builder.MapperBuilderAssistant;
import org.apache.ibatis.builder.ResultMapResolver;
import org.apache.ibatis.cache.Cache;
import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.mapping.Discriminator;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.mapping.ParameterMode;
import org.apache.ibatis.mapping.ResultFlag;
import org.apache.ibatis.mapping.ResultMap;
import org.apache.ibatis.mapping.ResultMapping;
import org.apache.ibatis.parsing.XNode;
import org.apache.ibatis.parsing.XPathParser;
import org.apache.ibatis.reflection.MetaClass;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;

/**
 * @author Clinton Begin
 * @author Kazuki Shimizu
 */
public class XMLMapperBuilder extends BaseBuilder {

	private final XPathParser parser;
	private final MapperBuilderAssistant builderAssistant;
	private final Map<String, XNode> sqlFragments;
	private final String resource;

	@Deprecated
	public XMLMapperBuilder(Reader reader, Configuration configuration, String resource,
			Map<String, XNode> sqlFragments, String namespace) {
		this(reader, configuration, resource, sqlFragments);
		this.builderAssistant.setCurrentNamespace(namespace);
	}

	@Deprecated
	public XMLMapperBuilder(Reader reader, Configuration configuration, String resource,
			Map<String, XNode> sqlFragments) {
		this(new XPathParser(reader, true, configuration.getVariables(), new XMLMapperEntityResolver()), configuration,
				resource, sqlFragments);
	}

	public XMLMapperBuilder(InputStream inputStream, Configuration configuration, String resource,
			Map<String, XNode> sqlFragments, String namespace) {
		this(inputStream, configuration, resource, sqlFragments);
		this.builderAssistant.setCurrentNamespace(namespace);
	}

	public XMLMapperBuilder(InputStream inputStream, Configuration configuration, String resource,
			Map<String, XNode> sqlFragments) {
		this(new XPathParser(inputStream, true, configuration.getVariables(), new XMLMapperEntityResolver()),
				configuration, resource, sqlFragments);
	}

	private XMLMapperBuilder(XPathParser parser, Configuration configuration, String resource,
			Map<String, XNode> sqlFragments) {
		super(configuration);
		this.builderAssistant = new MapperBuilderAssistant(configuration, resource);
		this.parser = parser;
		this.sqlFragments = sqlFragments;
		this.resource = resource;
	}

	public void parse() {
		// 若当前mapper没有被解析，开始解析
		// 如果当前mappers中有相同的mapper侧不再解析
		if (!configuration.isResourceLoaded(resource)) {
			// 解析mapper标签
			configurationElement(parser.evalNode("/mapper"));
			// 将mapper.xml 添加到configuration 中LoadedResource容器中，下次不再解析
			configuration.addLoadedResource(resource);
			//绑定映射器到namespace
			bindMapperForNamespace();
		}
		
		parsePendingResultMaps();
		parsePendingCacheRefs();
		parsePendingStatements();
	}

	public XNode getSqlFragment(String refid) {
		return sqlFragments.get(refid);
	}

	private void configurationElement(XNode context) {
		try {
			// 获取namespace属性值
			String namespace = context.getStringAttribute("namespace");
			// namespace 为必须值
			if (namespace == null || namespace.equals("")) {
				throw new BuilderException("Mapper's namespace cannot be empty");
			}
			// 将namespace的值赋值给 builderAssistant
			builderAssistant.setCurrentNamespace(namespace);
			// 解析</cache-ref> 节点
			cacheRefElement(context.evalNode("cache-ref"));
			// 解析</cache> 节点
			cacheElement(context.evalNode("cache"));
			// 解析</parameterMap> 节点
			parameterMapElement(context.evalNodes("/mapper/parameterMap"));
			// 解析</resultMap> 节点
			resultMapElements(context.evalNodes("/mapper/resultMap"));
			// 解析</sql> 节点
			sqlElement(context.evalNodes("/mapper/sql"));
			// 解析</select> </insert> </update> </delete> 节点 ？？ 重点
			buildStatementFromContext(context.evalNodes("select|insert|update|delete"));
		} catch (Exception e) {
			throw new BuilderException("Error parsing Mapper XML. The XML location is '" + resource + "'. Cause: " + e,
					e);
		}
	}

	private void buildStatementFromContext(List<XNode> list) {
		if (configuration.getDatabaseId() != null) {
			buildStatementFromContext(list, configuration.getDatabaseId());
		}
		buildStatementFromContext(list, null);
	}

	private void buildStatementFromContext(List<XNode> list, String requiredDatabaseId) {
		// 循环解析select|insert|update|delete 节点集合
		for (XNode context : list) {
			// 构建XMLStatementBuilder对象解析xml
			final XMLStatementBuilder statementParser = new XMLStatementBuilder(configuration, builderAssistant,
					context, requiredDatabaseId);
			try {
				statementParser.parseStatementNode();
			} catch (IncompleteElementException e) {
				configuration.addIncompleteStatement(statementParser);
			}
		}
	}

	private void parsePendingResultMaps() {
		Collection<ResultMapResolver> incompleteResultMaps = configuration.getIncompleteResultMaps();
		synchronized (incompleteResultMaps) {
			Iterator<ResultMapResolver> iter = incompleteResultMaps.iterator();
			while (iter.hasNext()) {
				try {
					iter.next().resolve();
					iter.remove();
				} catch (IncompleteElementException e) {
					// ResultMap is still missing a resource...
				}
			}
		}
	}

	private void parsePendingCacheRefs() {
		Collection<CacheRefResolver> incompleteCacheRefs = configuration.getIncompleteCacheRefs();
		synchronized (incompleteCacheRefs) {
			Iterator<CacheRefResolver> iter = incompleteCacheRefs.iterator();
			while (iter.hasNext()) {
				try {
					iter.next().resolveCacheRef();
					iter.remove();
				} catch (IncompleteElementException e) {
					// Cache ref is still missing a resource...
				}
			}
		}
	}

	private void parsePendingStatements() {
		Collection<XMLStatementBuilder> incompleteStatements = configuration.getIncompleteStatements();
		synchronized (incompleteStatements) {
			Iterator<XMLStatementBuilder> iter = incompleteStatements.iterator();
			while (iter.hasNext()) {
				try {
					iter.next().parseStatementNode();
					iter.remove();
				} catch (IncompleteElementException e) {
					// Statement is still missing a resource...
				}
			}
		}
	}

	private void cacheRefElement(XNode context) {
		if (context != null) {
			configuration.addCacheRef(builderAssistant.getCurrentNamespace(), context.getStringAttribute("namespace"));
			CacheRefResolver cacheRefResolver = new CacheRefResolver(builderAssistant,
					context.getStringAttribute("namespace"));
			try {
				cacheRefResolver.resolveCacheRef();
			} catch (IncompleteElementException e) {
				configuration.addIncompleteCacheRef(cacheRefResolver);
			}
		}
	}

	private void cacheElement(XNode context) {
		if (context != null) {
			String type = context.getStringAttribute("type", "PERPETUAL");
			Class<? extends Cache> typeClass = typeAliasRegistry.resolveAlias(type);
			String eviction = context.getStringAttribute("eviction", "LRU");
			Class<? extends Cache> evictionClass = typeAliasRegistry.resolveAlias(eviction);
			Long flushInterval = context.getLongAttribute("flushInterval");
			Integer size = context.getIntAttribute("size");
			boolean readWrite = !context.getBooleanAttribute("readOnly", false);
			boolean blocking = context.getBooleanAttribute("blocking", false);
			Properties props = context.getChildrenAsProperties();
			builderAssistant.useNewCache(typeClass, evictionClass, flushInterval, size, readWrite, blocking, props);
		}
	}

	// <parameterMap id="selectAuthor" type="org.apache.ibatis.domain.blog.Author">
	// <parameter property="id" />
	// </parameterMap>
	private void parameterMapElement(List<XNode> list) {
		// 循环解析 </parameterMap>节点
		for (XNode parameterMapNode : list) {
			// 获取id属性值
			String id = parameterMapNode.getStringAttribute("id");
			// 获取type属性值
			String type = parameterMapNode.getStringAttribute("type");
			// 实例化对象： 首先在typeAliasRegistry容器中查看是否已存在，如果存在直接返回
			// 否则使用classLoaderWrapper.classForName进行加载
			Class<?> parameterClass = resolveClass(type);
			// 解析</parameterMap>节点中</parameter>子节点
			List<XNode> parameterNodes = parameterMapNode.evalNodes("parameter");
			List<ParameterMapping> parameterMappings = new ArrayList<>();
			// 循环解析</parameter>节点集合
			for (XNode parameterNode : parameterNodes) {
				// 获取 property 属性值
				String property = parameterNode.getStringAttribute("property");
				// 获取 javaType 属性值
				String javaType = parameterNode.getStringAttribute("javaType");
				// 获取 jdbcType 属性值
				String jdbcType = parameterNode.getStringAttribute("jdbcType");
				// 获取 resultMap 属性值
				String resultMap = parameterNode.getStringAttribute("resultMap");
				// 获取 mode 属性值
				String mode = parameterNode.getStringAttribute("mode");
				// 获取 typeHandler 属性值
				String typeHandler = parameterNode.getStringAttribute("typeHandler");
				// 获取 numericScale 属性值
				Integer numericScale = parameterNode.getIntAttribute("numericScale");
				// 解析mode 参数
				ParameterMode modeEnum = resolveParameterMode(mode);
				// 同上 parameterClass 解析
				Class<?> javaTypeClass = resolveClass(javaType);
				// 获取jdbcType Enum 类型
				JdbcType jdbcTypeEnum = resolveJdbcType(jdbcType);
				Class<? extends TypeHandler<?>> typeHandlerClass = resolveClass(typeHandler);
				// 将</parameter>节点属性值注册到 MapperBuilderAssistant 容器中
				ParameterMapping parameterMapping = builderAssistant.buildParameterMapping(parameterClass, property,
						javaTypeClass, jdbcTypeEnum, resultMap, modeEnum, typeHandlerClass, numericScale);
				// 将parameterMapping 容器添加到 parameterMappings结合中
				parameterMappings.add(parameterMapping);
			}
			// 将解析好的 parameterMappings 添加到configuration 中parameterMaps容器中
			builderAssistant.addParameterMap(id, parameterClass, parameterMappings);
		}
	}

	// <resultMap id="selectAuthor" type="org.apache.ibatis.domain.blog.Author">
	// <id column="id" property="id" />
	// <result property="username" column="username" />
	// <result property="password" column="password" />
	// <result property="email" column="email" />
	// <result property="bio" column="bio" />
	// <result property="favouriteSection" column="favourite_section" />
	// </resultMap>
	private void resultMapElements(List<XNode> list) throws Exception {
		// 循环解析</resultMap>节点
		for (XNode resultMapNode : list) {
			try {
				resultMapElement(resultMapNode);
			} catch (IncompleteElementException e) {
				// ignore, it will be retried
			}
		}
	}

	private ResultMap resultMapElement(XNode resultMapNode) throws Exception {
		return resultMapElement(resultMapNode, Collections.emptyList(), null);
	}

	private ResultMap resultMapElement(XNode resultMapNode, List<ResultMapping> additionalResultMappings,
			Class<?> enclosingType) throws Exception {
		ErrorContext.instance().activity("processing " + resultMapNode.getValueBasedIdentifier());
		// 获取<ResultMap>上的type属性（即resultMap的返回值类型）
		String type = resultMapNode.getStringAttribute("type", resultMapNode.getStringAttribute("ofType",
				resultMapNode.getStringAttribute("resultType", resultMapNode.getStringAttribute("javaType"))));
		// 将返回类型type 实例化成class对象
		Class<?> typeClass = resolveClass(type);
		if (typeClass == null) {
			typeClass = inheritEnclosingType(resultMapNode, enclosingType);
		}
		Discriminator discriminator = null;
		// 用于存储</resultMap>下所有子节点
		List<ResultMapping> resultMappings = new ArrayList<>();
		resultMappings.addAll(additionalResultMappings);
		// 获取</resultMap>下的子节点集合
		List<XNode> resultChildren = resultMapNode.getChildren();
		// 循环处理
		for (XNode resultChild : resultChildren) {
			// 当前节点为</constructor>时、将其child节点装到resultMappings中
			if ("constructor".equals(resultChild.getName())) {
				processConstructorElement(resultChild, typeClass, resultMappings);
				// 若当前节点为</discriminator>，则进行条件判断，并将命中的子节点添加到resultMappings中去
			} else if ("discriminator".equals(resultChild.getName())) {
				discriminator = processDiscriminatorElement(resultChild, typeClass, resultMappings);
			} else {
				// flags仅用于区分当前节点是否是<id>或<idArg>，因为这两个节点的属性名为name，而其他节点的属性名为property
				List<ResultFlag> flags = new ArrayList<>();
				if ("id".equals(resultChild.getName())) {
					flags.add(ResultFlag.ID);
				}
				resultMappings.add(buildResultMappingFromContext(resultChild, typeClass, flags));
			}
		}
		String id = resultMapNode.getStringAttribute("id", resultMapNode.getValueBasedIdentifier());
		String extend = resultMapNode.getStringAttribute("extends");
		Boolean autoMapping = resultMapNode.getBooleanAttribute("autoMapping");
		
		// ResultMapResolver的作用是生成ResultMap对象，并将其加入到Configuration对象的resultMaps容器中（具体过程见下）
		ResultMapResolver resultMapResolver = new ResultMapResolver(builderAssistant, id, typeClass, extend,
				discriminator, resultMappings, autoMapping);
		try {
			return resultMapResolver.resolve();
		} catch (IncompleteElementException e) {
			configuration.addIncompleteResultMap(resultMapResolver);
			throw e;
		}
	}

	protected Class<?> inheritEnclosingType(XNode resultMapNode, Class<?> enclosingType) {
		if ("association".equals(resultMapNode.getName()) && resultMapNode.getStringAttribute("resultMap") == null) {
			String property = resultMapNode.getStringAttribute("property");
			if (property != null && enclosingType != null) {
				MetaClass metaResultType = MetaClass.forClass(enclosingType, configuration.getReflectorFactory());
				return metaResultType.getSetterType(property);
			}
		} else if ("case".equals(resultMapNode.getName()) && resultMapNode.getStringAttribute("resultMap") == null) {
			return enclosingType;
		}
		return null;
	}

	private void processConstructorElement(XNode resultChild, Class<?> resultType, List<ResultMapping> resultMappings)
			throws Exception {
		List<XNode> argChildren = resultChild.getChildren();
		for (XNode argChild : argChildren) {
			List<ResultFlag> flags = new ArrayList<>();
			flags.add(ResultFlag.CONSTRUCTOR);
			if ("idArg".equals(argChild.getName())) {
				flags.add(ResultFlag.ID);
			}
			resultMappings.add(buildResultMappingFromContext(argChild, resultType, flags));
		}
	}

	private Discriminator processDiscriminatorElement(XNode context, Class<?> resultType,
			List<ResultMapping> resultMappings) throws Exception {
		String column = context.getStringAttribute("column");
		String javaType = context.getStringAttribute("javaType");
		String jdbcType = context.getStringAttribute("jdbcType");
		String typeHandler = context.getStringAttribute("typeHandler");
		Class<?> javaTypeClass = resolveClass(javaType);
		Class<? extends TypeHandler<?>> typeHandlerClass = resolveClass(typeHandler);
		JdbcType jdbcTypeEnum = resolveJdbcType(jdbcType);
		Map<String, String> discriminatorMap = new HashMap<>();
		for (XNode caseChild : context.getChildren()) {
			String value = caseChild.getStringAttribute("value");
			String resultMap = caseChild.getStringAttribute("resultMap",
					processNestedResultMappings(caseChild, resultMappings, resultType));
			discriminatorMap.put(value, resultMap);
		}
		return builderAssistant.buildDiscriminator(resultType, column, javaTypeClass, jdbcTypeEnum, typeHandlerClass,
				discriminatorMap);
	}

	private void sqlElement(List<XNode> list) {
		if (configuration.getDatabaseId() != null) {
			sqlElement(list, configuration.getDatabaseId());
		}
		sqlElement(list, null);
	}
//	<sql id="Base_Column_List">
//    	id, proxy_addr, user_type, status, token, create_time, update_time
//    </sql>
	private void sqlElement(List<XNode> list, String requiredDatabaseId) {
		// 循环解析</sql>节点
		for (XNode context : list) {
			// 获取databaseId属性
			String databaseId = context.getStringAttribute("databaseId");
			// 获取id属性
			String id = context.getStringAttribute("id");
			// 将id 处理成： namespace.id的格式
			id = builderAssistant.applyCurrentNamespace(id, false);
			// 将结果存储到sqlFragments容器中
			if (databaseIdMatchesCurrent(id, databaseId, requiredDatabaseId)) {
				sqlFragments.put(id, context);
			}
		}
	}

	private boolean databaseIdMatchesCurrent(String id, String databaseId, String requiredDatabaseId) {
		if (requiredDatabaseId != null) {
			if (!requiredDatabaseId.equals(databaseId)) {
				return false;
			}
		} else {
			if (databaseId != null) {
				return false;
			}
			// skip this fragment if there is a previous one with a not null databaseId
			if (this.sqlFragments.containsKey(id)) {
				XNode context = this.sqlFragments.get(id);
				if (context.getStringAttribute("databaseId") != null) {
					return false;
				}
			}
		}
		return true;
	}

	private ResultMapping buildResultMappingFromContext(XNode context, Class<?> resultType, List<ResultFlag> flags)
			throws Exception {
		String property;
		if (flags.contains(ResultFlag.CONSTRUCTOR)) {
			property = context.getStringAttribute("name");
		} else {
			property = context.getStringAttribute("property");
		}
		String column = context.getStringAttribute("column");
		String javaType = context.getStringAttribute("javaType");
		String jdbcType = context.getStringAttribute("jdbcType");
		String nestedSelect = context.getStringAttribute("select");
		String nestedResultMap = context.getStringAttribute("resultMap",
				processNestedResultMappings(context, Collections.emptyList(), resultType));
		String notNullColumn = context.getStringAttribute("notNullColumn");
		String columnPrefix = context.getStringAttribute("columnPrefix");
		String typeHandler = context.getStringAttribute("typeHandler");
		String resultSet = context.getStringAttribute("resultSet");
		String foreignColumn = context.getStringAttribute("foreignColumn");
		boolean lazy = "lazy".equals(
				context.getStringAttribute("fetchType", configuration.isLazyLoadingEnabled() ? "lazy" : "eager"));
		Class<?> javaTypeClass = resolveClass(javaType);
		Class<? extends TypeHandler<?>> typeHandlerClass = resolveClass(typeHandler);
		JdbcType jdbcTypeEnum = resolveJdbcType(jdbcType);
		return builderAssistant.buildResultMapping(resultType, property, column, javaTypeClass, jdbcTypeEnum,
				nestedSelect, nestedResultMap, notNullColumn, columnPrefix, typeHandlerClass, flags, resultSet,
				foreignColumn, lazy);
	}

	private String processNestedResultMappings(XNode context, List<ResultMapping> resultMappings,
			Class<?> enclosingType) throws Exception {
		if ("association".equals(context.getName()) || "collection".equals(context.getName())
				|| "case".equals(context.getName())) {
			if (context.getStringAttribute("select") == null) {
				validateCollection(context, enclosingType);
				ResultMap resultMap = resultMapElement(context, resultMappings, enclosingType);
				return resultMap.getId();
			}
		}
		return null;
	}

	protected void validateCollection(XNode context, Class<?> enclosingType) {
		if ("collection".equals(context.getName()) && context.getStringAttribute("resultMap") == null
				&& context.getStringAttribute("javaType") == null) {
			MetaClass metaResultType = MetaClass.forClass(enclosingType, configuration.getReflectorFactory());
			String property = context.getStringAttribute("property");
			if (!metaResultType.hasSetter(property)) {
				throw new BuilderException("Ambiguous collection type for property '" + property
						+ "'. You must specify 'javaType' or 'resultMap'.");
			}
		}
	}

	private void bindMapperForNamespace() {
		String namespace = builderAssistant.getCurrentNamespace();
		if (namespace != null) {
			Class<?> boundType = null;
			try {
				boundType = Resources.classForName(namespace);
			} catch (ClassNotFoundException e) {
				// ignore, bound type is not required
			}
			if (boundType != null) {
				if (!configuration.hasMapper(boundType)) {
					// Spring may not know the real resource name so we set a flag
					// to prevent loading again this resource from the mapper interface
					// look at MapperAnnotationBuilder#loadXmlResource
					configuration.addLoadedResource("namespace:" + namespace);
					configuration.addMapper(boundType);
				}
			}
		}
	}

}
