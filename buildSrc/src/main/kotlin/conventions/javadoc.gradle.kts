/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package conventions

// Provides the Javadoc configuration for the build
plugins {
    id("java-library")
    id("maven-publish")
}

// Create a generic `docs` task
tasks.register("docs") {
    group = "documentation"
    dependsOn("javadoc")
}

tasks.withType<Javadoc> {
    exclude("**/com/mongodb/**/assertions/**")
    exclude("**/com/mongodb/**/internal/**")
    exclude("**/org/bson/**/internal/**")

    setDestinationDir(rootProject.file("build/docs/${project.base.archivesName.get()}"))

    val standardDocletOptions = options as StandardJavadocDocletOptions
    standardDocletOptions.apply {
        author(true)
        version(true)
        links =
            listOf(
                "https://docs.oracle.com/en/java/javase/11/docs/api/",
                "https://www.reactive-streams.org/reactive-streams-1.0.3-javadoc/")
        tagletPath(rootProject.projectDir.resolve("buildSrc/build/classes/java/main"))
        taglets("com.mongodb.doclet.AtlasManualTaglet")
        taglets("com.mongodb.doclet.ManualTaglet")
        taglets("com.mongodb.doclet.DochubTaglet")
        taglets("com.mongodb.doclet.ServerReleaseTaglet")
        encoding = "UTF-8"
        charSet("UTF-8")
        docEncoding("UTF-8")
        addBooleanOption("html5", true)
        addBooleanOption("-allow-script-in-comments", true)
        header(
            """
               <script type="text/javascript">
               function setLocationHash() {
                 try {
                   locationHash = top.location.hash;
                   if (window.name == "classFrame" && locationHash != "") {
                     window.location.hash = locationHash;
                   }
                 } catch (error) {}
               };
               function setGATrackingCode() {
                 if (window.name == "" || window.name == "classFrame") {
                     var _elqQ = _elqQ || [];
                     _elqQ.push(["elqSetSiteId", "413370795"]);
                     _elqQ.push(["elqTrackPageView"]);
                     (function () {
                     function async_load() { var s = document.createElement("script"); s.type = "text/javascript"; s.async = true; s.src = "//img03.en25.com/i/elqCfg.min.js"; var x = document.getElementsByTagName("script")[0]; x.parentNode.insertBefore(s, x); }
                     if (window.addEventListener) window.addEventListener("DOMContentLoaded", async_load, false);
                     else if (window.attachEvent) window.attachEvent("onload", async_load);
                     })();
                 }
               };
               function setSearchUrlPrefix() {
                 if (typeof getURLPrefix === "function") {
                   var getURLPrefixOri = getURLPrefix;
                   getURLPrefix = function(ui) {
                     var urlPrefix = getURLPrefixOri(ui);
                     return (urlPrefix && urlPrefix != "undefined/"  ? urlPrefix: "");
                   };
                 } else {
                   window.setTimeout(setSearchUrlPrefix, 500 );
                 }
               };
               setLocationHash();
               setGATrackingCode();
               setSearchUrlPrefix();
               </script>
        """.trimIndent())
    }

    // Customizations for specific projects
    afterEvaluate {
        val docVersion = docVersion(project.version as String)
        if (project.name != "bson") linksOfflineHelper(docVersion, "bson", standardDocletOptions)
        if (!project.name.contains("bson") && project.name != "mongodb-driver-core")
            linksOfflineHelper(docVersion, "mongodb-driver-core", standardDocletOptions)
        if (!project.name.contains("bson") && project.name != "mongodb-driver-sync")
            linksOfflineHelper(docVersion, "mongodb-driver-sync", standardDocletOptions)
    }
}

// Helper functions
internal fun docVersion(version: String): String {
    val (major, minor, patch) = version.split("-").first().split(".").map { it.toInt() }
    var docVersion = "${major}.${minor}"
    if (version.contains("-SNAPSHOT") && patch == 0 && minor > 0) {
        docVersion = "${major}.${minor - 1}"
    }
    return docVersion
}

internal fun linksOfflineHelper(docVersion: String, packageName: String, options: StandardJavadocDocletOptions): Unit {
    val docsPath = rootProject.file("build/docs/${packageName}")
    if (docsPath.exists()) {
        options.apply {
            linksOffline(
                "http://mongodb.github.io/mongo-java-driver/${docVersion}/apidocs/${packageName}/", docsPath.path)
        }
    }
}
