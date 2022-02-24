package org.dockstore.dockstore_snakemake_interface.language;

import com.google.common.io.Resources;
import io.dockstore.language.MinimalLanguageInterface;
import io.dockstore.language.RecommendedLanguageInterface;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

public class SnakeMakeWorkflowLanguagePluginTest {
  public static final String REPO_ID_1 = "dockstore-testing/SnakeMake-example";
  public static final String REPO_FORMAT_2 = "https://raw.githubusercontent.com/" + REPO_ID_1;
  public static final String EXAMPLE_FILENAME_1 = "Snakefile";
  public static final String EXAMPLE_FILENAME_1_PATH = "/workflow/" + EXAMPLE_FILENAME_1;
  public static final String EXAMPLE_FILENAME_2_PATH = "/" + EXAMPLE_FILENAME_1;
  public static final String CURRENT_BRANCH = "main";

  @Test
  public void testFormatWorkflowParsing() {
    final SnakeMakeWorkflowPlugin.SnakeMakeWorkflowPluginImpl plugin =
        new SnakeMakeWorkflowPlugin.SnakeMakeWorkflowPluginImpl();
    final HttpFileReader reader = new HttpFileReader(REPO_FORMAT_2);
    final String initialPath = EXAMPLE_FILENAME_1_PATH;
    final String contents = reader.readFile(EXAMPLE_FILENAME_1_PATH);
    final Map<String, Pair<String, MinimalLanguageInterface.GenericFileType>> fileMap =
        plugin.indexWorkflowFiles(initialPath, contents, reader);
    Assert.assertEquals(1, fileMap.size());
    final Pair<String, MinimalLanguageInterface.GenericFileType> discoveredFile =
        fileMap.get("/workflow/rules/bwa.smk");
    Assert.assertEquals(
        discoveredFile.getRight(), MinimalLanguageInterface.GenericFileType.IMPORTED_DESCRIPTOR);
    final RecommendedLanguageInterface.WorkflowMetadata metadata =
        plugin.parseWorkflowForMetadata(initialPath, contents, fileMap);
    // There is a doc for this workflow, use that for the description
    Assert.assertEquals("SnakeMake workflow description", metadata.getDescription());
  }

  @Test
  public void testInitialPathPattern() {
    // TODO: This doesn't seem to be called by Dockstore anywhere - is that right?
    final SnakeMakeWorkflowPlugin.SnakeMakeWorkflowPluginImpl plugin =
        new SnakeMakeWorkflowPlugin.SnakeMakeWorkflowPluginImpl();
    Matcher m = plugin.initialPathPattern().matcher(EXAMPLE_FILENAME_2_PATH);
    Assert.assertTrue("File name matches for initial path pattern", m.matches());
    m = plugin.initialPathPattern().matcher("/Dockerstore.cwl");
    Assert.assertFalse(m.matches());
    m = plugin.initialPathPattern().matcher("/Dockerstore.nf");
    Assert.assertFalse(m.matches());
  }

  abstract static class URLFileReader implements MinimalLanguageInterface.FileReader {
    // URL to repo
    protected final String repo;
    // extracted ID
    protected final Optional<String> id;

    URLFileReader(final String repo) {
      this.repo = repo;
      final String[] split = repo.split("/");
      if (split.length >= 2) {
        id = Optional.of(split[split.length - 2] + "/" + split[split.length - 1]);
      } else {
        id = Optional.empty();
      }
    }

    protected abstract URL getUrl(final String path) throws IOException;

    @Override
    public String readFile(String path) {
      try {
        if (path.startsWith("/")) {
          path = path.substring(1);
        }
        URL url = this.getUrl(path);
        return Resources.toString(url, StandardCharsets.UTF_8);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    // Get list of files in a directory (non-recursive) relative to the initial path
    @Override
    public List<String> listFiles(String pathToDirectory) {
      if (pathToDirectory.startsWith("/")) {
        pathToDirectory = pathToDirectory.substring(1);
      }

      // TODO implement
      return new ArrayList<>();
    }
  }

  static class ResourceFileReader extends URLFileReader {

    ResourceFileReader(final String repo) {
      super(repo);
    }

    @Override
    protected URL getUrl(String path) throws IOException {
      final String classPath = "repos/" + this.repo + "/" + path;
      final URL url = SnakeMakeWorkflowLanguagePluginTest.class.getResource(classPath);
      if (url == null) {
        throw new IOException("No such file " + classPath);
      }
      return url;
    }
  }

  static class HttpFileReader extends URLFileReader {

    HttpFileReader(final String repo) {
      super(repo);
    }

    @Override
    protected URL getUrl(final String path) throws IOException {
      return new URL(this.repo + "/" + CURRENT_BRANCH + "/" + path);
    }
  }
}
