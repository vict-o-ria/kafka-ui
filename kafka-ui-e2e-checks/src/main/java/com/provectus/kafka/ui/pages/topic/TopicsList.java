package com.provectus.kafka.ui.pages.topic;

import static com.codeborne.selenide.Selenide.$$x;
import static com.codeborne.selenide.Selenide.$x;
import static org.apache.commons.lang.math.RandomUtils.nextInt;

import com.codeborne.selenide.CollectionCondition;
import com.codeborne.selenide.Condition;
import com.codeborne.selenide.ElementsCollection;
import com.codeborne.selenide.SelenideElement;
import com.provectus.kafka.ui.pages.BasePage;
import io.qameta.allure.Step;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.experimental.ExtensionMethod;
import scala.Int;

public class TopicsList extends BasePage {

    protected SelenideElement topicListHeader = $x("//*[text()='Topics']");
    protected SelenideElement addTopicBtn = $x("//button[normalize-space(text()) ='Add a Topic']");
    protected SelenideElement searchField = $x("//input[@placeholder='Search by Topic Name']");
    protected SelenideElement showInternalRadioBtn = $x("//input[@name='ShowInternalTopics']");
    protected String actionButtonLocator = "//button[text()='%s']";
    protected ElementsCollection externalTopicGridItems = $$x("//td[@style='width: 1px;']/..//a");
  protected ElementsCollection internalTopicGridItems = $$x("//span[contains(text(),'IN')]");

    @Step
    public TopicsList waitUntilScreenReady() {
        waitUntilSpinnerDisappear();
        topicListHeader.shouldBe(Condition.visible);
        return this;
    }

    @Step
    public TopicsList clickAddTopicBtn() {
        clickByJavaScript(addTopicBtn);
        return this;
    }

    @Step
    public boolean isTopicVisible(String topicName) {
        tableGrid.shouldBe(Condition.visible);
        return isVisible(getTableElement(topicName));
    }

    public boolean isInternalTopicVisible(){
      return isVisible(internalTopicGridItems.first());
    }

//  private List<TopicGridItems> initExternalItems() {
//    List<TopicGridItems> gridItemList = new ArrayList<>();
//    externalTopicGridItems.shouldHave(CollectionCondition.sizeGreaterThan(0))
//        .forEach(item -> gridItemList.add(new TopicGridItems(item)));
//    return gridItemList;
//  }

  private List<TopicGridItems> initInternalItems() {
    List<TopicGridItems> gridItemList = new ArrayList<>();
    internalTopicGridItems.shouldHave(CollectionCondition.sizeGreaterThan(0))
        .forEach(item -> gridItemList.add(new TopicGridItems(item)));
    return gridItemList;
  }

//  @Step
//  public TopicGridItems getTopicEx() {
//    return initExternalItems().stream()
//        .findFirst().orElse(null);
//  }

  @Step
  public TopicGridItems getTopicIn() {
    return initInternalItems().stream()
        .findFirst().orElse(null);
  }

  @Step
  public TopicGridItems getInternalTopic() {
    return getTopicIn();
  }

//  @Step
//  public TopicGridItems getExternalTopic() {
//    return getTopicEx();
//  }

public static class TopicGridItems extends BasePage {

  private final SelenideElement element;

  public TopicGridItems(SelenideElement element) {
    this.element = element;
  }

  private SelenideElement getTopicRowElm() {
    return element.$x("//td[@style='width: 1px;']/..//a");
  }

  @Step
  public int getTopicRow() {
    return Integer.parseInt(getTopicRowElm().getText().trim());
  }

}

    @Step
    public boolean isInternalRadioBtnSelected(){
      return isSelected(showInternalRadioBtn);
    }

    @Step
    public TopicsList clickInternalRadioButton(){
      clickByJavaScript(showInternalRadioBtn);
      return this;
    }

    @Step
    public TopicsList openTopic(String topicName) {
        getTableElement(topicName).shouldBe(Condition.enabled).click();
        return this;
    }

    private List<SelenideElement> getActionButtons() {
      return Stream.of("Delete selected topics", "Copy selected topic", "Purge messages of selected topics")
          .map(name -> $x(String.format(actionButtonLocator, name)))
          .collect(Collectors.toList());
    }

    private List<SelenideElement> getVisibleColumnHeaders() {
      return Stream.of("Replication Factor","Number of messages","Topic Name", "Partitions", "Out of sync replicas", "Size")
          .map(name -> $x(String.format(columnHeaderLocator, name)))
        .collect(Collectors.toList());
    }

    private List<SelenideElement> getEnabledColumnHeaders(){
      return Stream.of("Topic Name", "Partitions", "Out of sync replicas", "Size")
          .map(name -> $x(String.format(columnHeaderLocator, name)))
          .collect(Collectors.toList());
    }

    @Step
    public List<SelenideElement> getAllVisibleElements() {
      List<SelenideElement> visibleElements = new ArrayList<>(getVisibleColumnHeaders());
      visibleElements.addAll(Arrays.asList(searchField, addTopicBtn, tableGrid));
      visibleElements.addAll(getActionButtons());
      return visibleElements;
    }

    @Step
    public List<SelenideElement> getAllEnabledElements() {
      List<SelenideElement> enabledElements = new ArrayList<>(getEnabledColumnHeaders());
      enabledElements.addAll(Arrays.asList(searchField, showInternalRadioBtn,addTopicBtn));
      return enabledElements;
    }
}
