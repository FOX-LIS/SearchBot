package searchengine.data;

import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import searchengine.config.SiteItem;
import searchengine.config.UserData;
import searchengine.model.*;
import searchengine.repositories.*;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

@RequiredArgsConstructor
public class TotalIndexer extends Thread {
    private final FieldRepository fieldRepository;
    private final IndexRepository indexRepository;
    private final LemmaRepository lemmaRepository;
    private final PageRepository pageRepository;
    private final SiteRepository siteRepository;
    private final List<SiteItem> yamlSites;
    private final UserData userData;
    ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private volatile boolean isStopped = false;
    private final Logger logger = LogManager.getLogger(getClass());

    public synchronized void setIsStopped(boolean isStopped) {
        this.isStopped = isStopped;
        if (this.isStopped) {
            try {
                executor.shutdownNow();
                currentThread().interrupt();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    @Override
    public void run() {
        if (!runTotalIndexing()) {
            String message = "Индексация остановлена пользователем";
            logger.info(message);
            siteRepository.findAll().forEach(site -> {
                site.setLast_error(message);
                site.setStatus(Status.FAILED);
                new PagesIndexer(fieldRepository, indexRepository, lemmaRepository, pageRepository, siteRepository,
                        new AtomicBoolean(false)).saveSite(site);
            });
        }
    }

    private boolean runTotalIndexing() {
        isStopped = false;
        if (!updatingSites()) {
            return false;
        }
        for (SiteItem siteItem : yamlSites) {
            if (!runSiteIndexing(siteItem)) {
                return false;
            }
        }
        return true;
    }

    private boolean runSiteIndexing(SiteItem siteItem) {
        String url = siteItem.getUrl();
        String siteName = siteItem.getName();
        Site site = siteRepository.findByUrl(url);
        try {
            boolean isDeletedOk = executor.submit(() -> deleteIndexingSiteInfo(site)).get();
            if (!isDeletedOk) {
                return false;
            }
        } catch (InterruptedException | ExecutionException e) {
            return false;
        }
        if (isStopped) {
            return false;
        }
        OneSiteIndexer oneSiteIndexer = new OneSiteIndexer(fieldRepository, indexRepository, lemmaRepository, pageRepository, siteRepository,
                new AtomicBoolean(false));
        oneSiteIndexer.setROOT_URL(url);
        oneSiteIndexer.setROOT_URL_NAME(siteName);
        oneSiteIndexer.setUserData(userData);
        if (isStopped) {
            oneSiteIndexer.setIsInterrupted(true);
            return false;
        }
        int coresCount = Runtime.getRuntime().availableProcessors();
        executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(coresCount);
        executor.execute(oneSiteIndexer);
        return true;
    }

    private boolean updatingSites() {
        logger.info("Обновление информации о сайтах начато!");
        if (!updateSites()) {
            return false;
        }
        logger.info("Информация о сайтах обновлена!");
        return true;
    }

    private boolean deleteIndexingSiteInfo(Site site) {
        if ((isStopped) || !(deleteSiteInfo(site))) {
            return false;
        }
        logger.info("Удаление информации из БД об индексируемом сайте \"" + site.getName() + "\" закончено!");
        return true;
    }

    private boolean deleteSiteInfo(Site site) {
        List<Lemma> lemmaListToDelete = lemmaRepository.findAllBySiteLemma(site);
        lemmaRepository.deleteAll(lemmaListToDelete);
        String infoPrefix = "Сайт \"" + site.getName() + "\": ";
        logger.info(infoPrefix + "удаление лемм завершено!");
        if (isStopped) {
            return false;
        }
        List<Page> pageListToDelete = pageRepository.findBySitePage(site);
        pageRepository.deleteAll(pageListToDelete);
        logger.info(infoPrefix + "удаление страниц завершено!");
        if (isStopped) {
            return false;
        }
        List<Integer> pageIdListToDelete = pageListToDelete.stream().map(Page::getId).toList();
        List<Index> indexListToDelete = indexRepository.findAllByPageIdIn(pageIdListToDelete);
        indexRepository.deleteAll(indexListToDelete);
        logger.info(infoPrefix + "удаление индексов завершено!");
        return true;
    }

    private boolean updateSites() {
        if (!addingNewSites()) {
            return false;
        }
        if (isStopped) {
            return false;
        }
        return deletingSites();
    }

    private boolean addingNewSites() {
        Iterable<Site> sitesInDB = siteRepository.findAll();
        for (SiteItem yamlSite : yamlSites) {
            processYamlSite(sitesInDB, yamlSite);
            if (isStopped) {
                return false;
            }
        }
        return true;
    }

    private void processYamlSite(Iterable<Site> dbSites, SiteItem yamlSite) {
        boolean isPresent = false;
        for (Site dbSite : dbSites) {
            if (yamlSite.getUrl().equals(dbSite.getUrl())) {
                isPresent = true;
                break;
            }
        }
        Site site = new Site(Status.INDEXING, new Date(), yamlSite.getUrl(), yamlSite.getName());
        new PagesIndexer(fieldRepository, indexRepository, lemmaRepository, pageRepository, siteRepository,
                new AtomicBoolean(false)).saveSite(site);
        if (!isPresent) {
            logger.info("Добавлен новый сайт \"" + yamlSite.getUrl() + "\" в БД!");
        }
    }

    private boolean deletingSites() {
        Iterable<Site> dbSites = siteRepository.findAll();
        for (Site dbSite : dbSites) {
            processDBSite(dbSite);
            if (isStopped) {
                return false;
            }
        }
        return true;
    }

    private void processDBSite(Site dbSite) {
        boolean isPresent = false;
        for (SiteItem siteItem : yamlSites) {
            if (dbSite.getUrl().equals(siteItem.getUrl())) {
                isPresent = true;
                break;
            }
        }
        if (!isPresent) {
            siteRepository.delete(dbSite);
            logger.info("Удалён неиндексируемый сайт \"" + dbSite.getUrl() + "\" из БД!");
        }
    }
}
