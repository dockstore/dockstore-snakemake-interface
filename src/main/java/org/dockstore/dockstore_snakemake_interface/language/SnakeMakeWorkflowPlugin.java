package org.dockstore.dockstore_snakemake_interface.language;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import io.dockstore.common.DescriptorLanguage;
import io.dockstore.common.VersionTypeValidation;
import io.dockstore.language.CompleteLanguageInterface;
import io.dockstore.language.RecommendedLanguageInterface;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.pf4j.Extension;
import org.pf4j.Plugin;
import org.pf4j.PluginWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// import org.dockstore.smk.Cytoscape;

/** @author wshands */
public class SnakeMakeWorkflowPlugin extends Plugin {
  public static final Logger LOG = LoggerFactory.getLogger(SnakeMakeWorkflowPlugin.class);
  public static final String SNAKEFILE_EXTENSION = ".smk";

  /**
   * Constructor to be used by plugin manager for plugin instantiation. Your plugins have to provide
   * constructor with this exact signature to be successfully loaded by manager.
   *
   * @param wrapper
   */
  public SnakeMakeWorkflowPlugin(PluginWrapper wrapper) {
    super(wrapper);
  }

  @Extension
  public static class SnakeMakeWorkflowPluginImpl implements CompleteLanguageInterface {

    @Override
    public String launchInstructions(String trsID) {
      return null;
    }

    // Process a workflow and generate a cytoscape compatible data structure
    // http://manual.cytoscape.org/en/stable/Supported_Network_File_Formats.html#cytoscape-js-json
    @Override
    public Map<String, Object> loadCytoscapeElements(
        String initialPath,
        String contents,
        Map<String, Pair<String, GenericFileType>> indexedFiles) {
      final Map<String, Object> workflow = loadWorkflow(contents);

      // TODO put Cytoscape back or another parser that can parse Snakemake files and return a map
      // of strings to objects
      return workflow;
      // return Cytoscape.getElements(workflow);
    }

    // Generate table containing information on the steps of the workflow, potentially including
    // ids, URLs to more information, Docker containers
    @Override
    public List<RowData> generateToolsTable(
        String initialPath,
        String contents,
        Map<String, Pair<String, GenericFileType>> indexedFiles) {
      // When a Snakemake parser is hooked up you can uncomment this
      // code to return the map for the Snakemake file
      // final Map<String, Object> workflow = loadWorkflow(contents);
      // final Map<String, Object> elements = Cytoscape.getElements(workflow);
      final Map<String, Object> elements = (Map<String, Object>) new HashMap<String, Object>();
      final List<Map> nodes = (List<Map>) elements.getOrDefault("nodes", Lists.newArrayList());
      // removeStartAndEndNodes(nodes);
      return nodes.stream()
          .map(
              node -> {
                final RowData rowData = new RowData();
                final Map<String, Object> nodeData = (Map<String, Object>) node.get("data");
                rowData.label = (String) nodeData.getOrDefault("label", "");
                rowData.dockerContainer = (String) nodeData.getOrDefault("docker", "");
                rowData.filename = (String) nodeData.getOrDefault("run", "");
                // TODO: get a sane link here when Docker is hooked up
                try {
                  rowData.link = new URL((String) node.getOrDefault("repo_link", ""));
                } catch (MalformedURLException e) {
                  rowData.link = null;
                }
                rowData.rowType = RowType.TOOL;
                rowData.toolid = (String) nodeData.getOrDefault("id", "");
                return rowData;
              })
          .collect(Collectors.toList());
    }

    @Override
    public VersionTypeValidation validateWorkflowSet(
        String initialPath,
        String contents,
        Map<String, Pair<String, GenericFileType>> indexedFiles) {

      // Add code to validate the indexed Snakemake files
      return new VersionTypeValidation(true, new HashMap<>());
    }

    @Override
    public VersionTypeValidation validateTestParameterSet(
        Map<String, Pair<String, GenericFileType>> indexedFiles) {
      return new VersionTypeValidation(true, new HashMap<>());
    }

    @Override
    public DescriptorLanguage getDescriptorLanguage() {
      return DescriptorLanguage.SMK;
    }

    @Override
    public Pattern initialPathPattern() {
      return Pattern.compile("/(Snakefile)");
    }

    @Override
    public Map<String, Pair<String, GenericFileType>> indexWorkflowFiles(
        final String initialPath, final String contents, final FileReader reader) {
      // An example of finding other subordinate descriptors pulled into the main descriptor
      // This will need to be fleshed out for a real Snakemake plugin
      final List<String> includedWorkflowFileNames = findIncludedRuleFileNames(contents);

      final int extensionPos = initialPath.lastIndexOf("/");
      final String base = initialPath.substring(0, extensionPos);

      Map<String, Pair<String, GenericFileType>> results =
          includedWorkflowFileNames.stream()
              // Enforce rule files  have the proper extension
              .filter(f -> f.endsWith(SNAKEFILE_EXTENSION))
              // Gather the paths and content of the files after all 'include:' keywords
              // into a map
              .collect(
                  Collectors.toMap(
                      fn -> base + "/" + fn,
                      fn ->
                          new ImmutablePair<>(
                              reader.readFile(base + "/" + fn),
                              GenericFileType.IMPORTED_DESCRIPTOR)));
      return results;
    }

    public static List<String> findIncludedRuleFileNames(String snakefileContents) {
      // Capture the path to included Snakefile(s)
      Pattern pattern = Pattern.compile("\\binclude:\\s++\"\\s?([^\\\\n\\s\"]++)*+");
      Matcher matcher = pattern.matcher(snakefileContents);
      List<String> matches = new ArrayList<String>();
      while (matcher.find()) {
        matches.add(matcher.group(1));
      }
      return matches;
    }

    protected List<String> findWorkflowFiles(
        final String initialPath, String contents, final FileReader reader) {
      final int extensionPos = initialPath.lastIndexOf("/");
      final String base = initialPath.substring(0, extensionPos);
      final Path parent = Paths.get(initialPath).getParent();

      List<String> extraWorkflowFilesContenders = reader.listFiles(parent.toString());
      List<String> extraWorkflowFiles =
          extraWorkflowFilesContenders.stream()
              .filter(f -> f.endsWith(SNAKEFILE_EXTENSION))
              .map(f -> reader.readFile(f))
              .collect(Collectors.toList());

      return extraWorkflowFiles;
    }

    static Map<String, Object> loadWorkflow(final String content) {
      // When a parser for Snakemake files is hooked up this may
      // be necessary to convert the map of the Snakemake file
      // to yaml
      // final Yaml yaml = new Yaml();
      // final Map map = yaml.loadAs(content, Map.class);
      // return (Map<String, Object>) map;

      return null;
    }

    // Given the primary descriptor and the files indexed from indexWorkflowFiles,
    // return relevant metadata about the workflow.
    @Override
    public RecommendedLanguageInterface.WorkflowMetadata parseWorkflowForMetadata(
        String initialPath,
        String content,
        Map<String, Pair<String, GenericFileType>> indexedFiles) {
      RecommendedLanguageInterface.WorkflowMetadata metadata =
          new RecommendedLanguageInterface.WorkflowMetadata();
      if (content != null && !content.isEmpty()) {
        metadata.setDescription("SnakeMake workflow description");
      }
      return metadata;
    }
  }
}
